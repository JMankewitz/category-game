const sqlite3 = require('sqlite3').verbose();

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Serve static files from public directory
app.use(express.static('public'));

// In-memory storage for rooms
const rooms = new Map();

// Initialize database
const dbPath = path.join(__dirname, 'game_data.db');
const db = new sqlite3.Database(dbPath);

// Enable foreign keys
db.run('PRAGMA foreign_keys = ON');

class GameTimer {
    constructor(duration, onComplete, onTick = null) {
        this.duration = duration;
        this.remaining = duration;
        this.onComplete = onComplete;
        this.onTick = onTick;
        this.interval = null;
        this.isActive = false;
    }
    
    start() {
        if (this.isActive) return;
        
        this.isActive = true;
        this.interval = setInterval(() => {
            this.remaining--;
            
            if (this.onTick) {
                this.onTick(this.remaining);
            }
            
            if (this.remaining <= 0) {
                this.complete();
            }
        }, 1000);
    }
    
    complete() {
        if (!this.isActive) return;
        
        this.isActive = false;
        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }
        this.onComplete();
    }
    
    cancel() {
        this.isActive = false;
        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }
    }
}


// Generate random room codes
function generateRoomCode() {
    const chars = 'ABCDEFGHIJKLMNPQRSTUVWXYZ123456789'; // No O, 0 to avoid confusion
    let result = '';
    for (let i = 0; i < 4; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

// Create new room structure
function createRoom(code) {
    return {
        code: code,
        dbGameId: null,
        players: new Map(),
        gmSocketId: null,
        displaySocketId: null,
        gameState: 'lobby', // Start in lobby instead of waiting
        currentCategory: '',
        submissions: [],
        round: 0,
        createdAt: new Date(),
        
        // Category management
        categorySubmissions: [], // Array of {playerId, nickname, category}
        availableCategories: [], // Cached list of available categories
        
        // Timer properties
        currentTimer: null,
        phaseStartTime: null,
        timerSettings: {
            submission: 120,
            votingPerExemplar: 15,
            votingMinimum: 30,
            exemplarResult: 5,
            summary: 15,
            scoreboard: 10
        }
    };
}

function broadcastTimerUpdate(room, remaining, phase) {
    const timerData = {
        remaining: remaining,
        phase: phase,
        gameState: room.gameState
    };
    
    // Send to all players
    io.to(room.code).emit('timer-update', timerData);
    
    // Send to display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('timer-update', timerData);
    }
}

async function selectNextCategory(room) {
    try {
        const availableCategories = await getAvailableCategories(room.dbGameId);
        
        if (availableCategories.length === 0) {
            console.warn(`No categories available for room ${room.code}`);
            return null;
        }
        
        // Simple random selection
        const selectedCategory = availableCategories[Math.floor(Math.random() * availableCategories.length)];
        
        // Mark as used
        await markCategoryAsUsed(room.dbGameId, selectedCategory);
        
        console.log(`Auto-selected category "${selectedCategory}" for room ${room.code}`);
        return selectedCategory;
    } catch (error) {
        console.error('Error selecting category:', error);
        return null;
    }
}

function startVotingPhase(room) {
    room.gameState = 'voting';
    
    // Cancel any existing timer
    if (room.currentTimer) {
        room.currentTimer.cancel();
    }
    
    // Calculate voting time based on number of exemplars
    const votingTime = Math.max(
        room.timerSettings.votingMinimum,
        room.submissions.length * room.timerSettings.votingPerExemplar
    );
    
    // Reset voting status
    room.players.forEach(player => {
        player.hasVoted = false;
    });

    // Clear existing votes
    room.submissions.forEach(submission => {
        submission.votes.clear();
    });
    
    // Start voting timer
    room.currentTimer = new GameTimer(
        votingTime,
        () => {
            console.log(`Voting timer expired for room ${room.code}`);
            startResultsPhase(room);
        },
        (remaining) => {
            broadcastTimerUpdate(room, remaining, 'voting');
        }
    );
    
    room.currentTimer.start();

    const gameStateData = {
        gameState: room.gameState,
        currentCategory: room.currentCategory,
        timerRemaining: votingTime,
        submissions: room.submissions.map(s => ({
            exemplar: s.exemplar,
            submittedBy: s.nickname
        })),
        players: Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted
        }))
    };

    io.to(room.code).emit('game-state-update', gameStateData);
    
    // Update display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('display-update', {
            gameState: room.gameState,
            currentCategory: room.currentCategory,
            totalPlayers: room.players.size,
            votedCount: 0,
            timerRemaining: votingTime
        });
    }
}

// Check if all players have voted (early completion)
function checkVotingComplete(room) {
  const votedCount = Array.from(room.players.values())
    .filter(p => p.hasVoted).length;

  const allVoted = (votedCount === room.players.size && room.players.size > 0);

  if (allVoted) {
    console.log(`All players voted early in room ${room.code}`);

    if (room.currentTimer) {
      room.currentTimer.cancel();
    }

    // Move to results immediately
    startResultsPhase(room);
    return true;               // <— signal completion
  }

  return false;
}

function startSubmissionPhase(room, category) {
    room.gameState = 'submitting';
    room.currentCategory = category;
    room.phaseStartTime = Date.now();
    room.submissions = [];
    
    // Reset player status
    room.players.forEach(player => {
        player.hasSubmitted = false;
        player.hasVoted = false;
    });
    
    // Cancel any existing timer
    if (room.currentTimer) {
        room.currentTimer.cancel();
    }
    
    // Start submission timer
    room.currentTimer = new GameTimer(
        room.timerSettings.submission,
        () => {
            console.log(`Submission timer expired for room ${room.code}`);
            startVotingPhase(room);
        },
        (remaining) => {
            broadcastTimerUpdate(room, remaining, 'submission');
        }
    );
    
    room.currentTimer.start();
    
    // Broadcast game state
    const gameStateData = {
        gameState: room.gameState,
        currentCategory: room.currentCategory,
        round: room.round,
        timerRemaining: room.timerSettings.submission,
        players: Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted
        }))
    };

    io.to(room.code).emit('game-state-update', gameStateData);
    
    // Update display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('display-update', {
            gameState: room.gameState,
            currentCategory: room.currentCategory,
            round: room.round,
            totalPlayers: room.players.size,
            submittedCount: 0,
            timerRemaining: room.timerSettings.submission
        });
    }
}

// Check if all players have submitted (early completion)
function checkSubmissionComplete(room) {
    const submittedCount = Array.from(room.players.values())
        .filter(p => p.hasSubmitted).length;
    
    if (submittedCount === room.players.size && room.players.size > 0) {
        console.log(`All players submitted early in room ${room.code}`);
        
        // Cancel the timer and directly start voting
        if (room.currentTimer) {
            room.currentTimer.cancel();
        }
        
        // Start voting phase immediately
        startVotingPhase(room);
    }
}

// Auto-results phase (replaces manual GM control)
async function startResultsPhase(room) {
    room.gameState = 'results';
    
    // Cancel any existing timer
    if (room.currentTimer) room.currentTimer.cancel();
    
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('display-update', {
            gameState: 'results',
            currentCategory: room.currentCategory,
            totalPlayers: room.players.size
        });
    }

    // Calculate scores and results (existing logic)
    const results = [];
    for (const submission of room.submissions) {
        const votes = Array.from(submission.votes.entries()).map(([playerId, vote]) => ({
            playerId,
            vote
        }));
        
        const yesCount = votes.filter(v => v.vote).length;
        const noCount = votes.length - yesCount;
        const points = Math.min(yesCount, noCount);
        
        // Award points to submitter
        const submitter = room.players.get(submission.playerId);
        if (submitter) {
            submitter.score += points;
            await updatePlayerFinalScore(submitter.socketId, room.dbGameId, submitter.score);
            await updateSubmissionResults(points, yesCount, noCount, room.currentRoundDbId, submitter.dbPlayerId);
        }
        
        results.push({
            exemplar: submission.exemplar,
            submittedBy: submission.nickname,
            votes: votes,
            yesCount,
            noCount,
            points
        });
    }

    room.currentResults = results;
    room.currentResultIndex = -1;

    // Notify players
    const gameStateData = {
        gameState: room.gameState,
        players: Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted
        }))
    };

    io.to(room.code).emit('game-state-update', gameStateData);
    
    // Initialize results mode on display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('results-mode-start', {
            totalResults: results.length
        });
    }

    // Start auto-advancing through results
    setTimeout(() => autoAdvanceResults(room), 150);
}

// Auto-advance through results
function autoAdvanceResults(room) {
    if (!room.currentResults || room.currentResultIndex >= room.currentResults.length - 1) {
        // All results shown, show summary
        setTimeout(() => autoShowSummary(room), 1000);
        return;
    }
    
    room.currentResultIndex++;
    const result = room.currentResults[room.currentResultIndex];
    
    // Send to display
    if (room.displaySocketId) {
        console.log(`Emitting exemplar ${room.currentResultIndex + 1}/${room.currentResults.length} to display`);
        io.to(room.displaySocketId).emit('show-exemplar-result', {
            exemplar: result.exemplar,
            submittedBy: result.submittedBy,
            votes: result.votes,
            yesCount: result.yesCount,
            noCount: result.noCount,
            points: result.points,
            currentIndex: room.currentResultIndex,
            totalResults: room.currentResults.length
        });
    }
    
    // Schedule next result
    setTimeout(() => autoAdvanceResults(room), room.timerSettings.exemplarResult * 1000);
}

// Auto-show summary
function autoShowSummary(room) {
    // Sort by points for summary (existing logic)
    const sortedByPoints = [...room.currentResults].sort((a, b) => {
        if (a.points !== b.points) {
            return b.points - a.points;
        }
        const aControversy = Math.abs(a.yesCount - a.noCount);
        const bControversy = Math.abs(b.yesCount - b.noCount);
        return aControversy - bControversy;
    });

    let displayData;
    if (sortedByPoints.length <= 6) {
        displayData = {
            showAll: true,
            allResults: sortedByPoints,
            title: `All ${sortedByPoints.length} Exemplars (Most to Least Points)`
        };
    } else {
        displayData = {
            showAll: false,
            topResults: sortedByPoints.slice(0, 3),
            bottomResults: sortedByPoints.slice(-3),
            title: 'Top & Bottom Scoring Exemplars'
        };
    }

    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('show-enhanced-summary', displayData);
    }
    
    // Schedule scoreboard
    setTimeout(() => autoShowScoreboard(room), room.timerSettings.summary * 1000);
}

// Auto-show scoreboard
function autoShowScoreboard(room) {
    const sortedPlayers = Array.from(room.players.values())
        .map(p => ({ nickname: p.nickname, score: p.score }))
        .sort((a, b) => b.score - a.score);

    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('show-round-scoreboard', {
            players: sortedPlayers,
            round: room.round,
            isGameWide: true
        });
    }
    
    // Schedule next round (this will be replaced with category management later)
    setTimeout(() => prepareNextRound(room), room.timerSettings.scoreboard * 1000);
}

// Prepare next round (placeholder for category management)
async function prepareNextRound(room) {
    room.round++;
    
    // Try to auto-select next category
    const nextCategory = await selectNextCategory(room);
    
    if (nextCategory) {
        // Start round automatically with selected category
        room.currentRoundDbId = await logRoundStarted(room.dbGameId, room.round, nextCategory);
        startSubmissionPhase(room, nextCategory);
        console.log(`Round ${room.round} started automatically with category "${nextCategory}"`);
    } else {
        // No categories available - wait for host to add more
        room.gameState = 'waiting-for-category';
        room.currentCategory = '';
        room.submissions = [];
        
        // Reset player status
        room.players.forEach(player => {
            player.hasSubmitted = false;
            player.hasVoted = false;
        });

        const gameStateData = {
            gameState: room.gameState,
            round: room.round,
            needsMoreCategories: true,
            players: Array.from(room.players.values()).map(p => ({
                nickname: p.nickname,
                score: p.score,
                hasSubmitted: p.hasSubmitted,
                hasVoted: p.hasVoted
            }))
        };

        io.to(room.code).emit('game-state-update', gameStateData);
        
        // Update display to show category needed
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
                gameState: room.gameState,
                round: room.round,
                totalPlayers: room.players.size,
                needsMoreCategories: true
            });
        }

        console.log(`Round ${room.round} waiting - no categories available in room ${room.code}`);
    }
}

// Create tables if they don't exist
function initializeDatabase() {
    return new Promise((resolve, reject) => {
        const schemaSQL = `
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_code TEXT NOT NULL UNIQUE,
            gm_id TEXT,
            started_at DATETIME,
            ended_at DATETIME,
            total_rounds INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        socket_id TEXT NOT NULL,
        nickname TEXT NOT NULL,
        game_id INTEGER NOT NULL,
        final_score INTEGER DEFAULT 0,
        joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        left_at DATETIME,
        FOREIGN KEY (game_id) REFERENCES games(id),
        UNIQUE(socket_id, game_id)
    );

    CREATE TABLE IF NOT EXISTS rounds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        round_number INTEGER NOT NULL,
        category TEXT NOT NULL,
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        submission_ended_at DATETIME,
        voting_ended_at DATETIME,
        results_shown_at DATETIME,
        total_submissions INTEGER DEFAULT 0,
        total_votes INTEGER DEFAULT 0,
        FOREIGN KEY (game_id) REFERENCES games(id),
        UNIQUE(game_id, round_number)
    );

    CREATE TABLE IF NOT EXISTS submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        round_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        exemplar TEXT NOT NULL,
        submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        points_earned INTEGER DEFAULT 0,
        yes_votes INTEGER DEFAULT 0,
        no_votes INTEGER DEFAULT 0,
        FOREIGN KEY (round_id) REFERENCES rounds(id),
        FOREIGN KEY (player_id) REFERENCES players(id),
        UNIQUE(round_id, player_id)
    );

    CREATE TABLE IF NOT EXISTS votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        submission_id INTEGER NOT NULL,
        voter_player_id INTEGER NOT NULL,
        vote BOOLEAN NOT NULL,
        voted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (submission_id) REFERENCES submissions(id),
        FOREIGN KEY (voter_player_id) REFERENCES players(id),
        UNIQUE(submission_id, voter_player_id)
    );

    CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    submitted_by_player_id INTEGER,
    category_text TEXT NOT NULL,
    is_preset BOOLEAN DEFAULT 0,
    was_used BOOLEAN DEFAULT 0,
    submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(id),
    FOREIGN KEY (submitted_by_player_id) REFERENCES players(id)
    );

    CREATE INDEX IF NOT EXISTS idx_categories_game_id ON categories(game_id);
    CREATE INDEX IF NOT EXISTS idx_categories_used ON categories(was_used);

    CREATE INDEX IF NOT EXISTS idx_games_room_code ON games(room_code);
    CREATE INDEX IF NOT EXISTS idx_players_game_id ON players(game_id);
    CREATE INDEX IF NOT EXISTS idx_rounds_game_id ON rounds(game_id);
    CREATE INDEX IF NOT EXISTS idx_submissions_round_id ON submissions(round_id);
    CREATE INDEX IF NOT EXISTS idx_submissions_player_id ON submissions(player_id);
    CREATE INDEX IF NOT EXISTS idx_votes_submission_id ON votes(submission_id);
    CREATE INDEX IF NOT EXISTS idx_votes_voter_id ON votes(voter_player_id);
    `;

    // Execute each statement separately
    db.exec(schemaSQL, (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

// Initialize database on startup
initializeDatabase();


// Database helper functions
function logGameCreated(roomCode, gmSocketId) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`INSERT INTO games (room_code, gm_id, started_at, status) VALUES (?, ?, ?, ?)`);
        stmt.run(roomCode, gmSocketId, new Date().toISOString(), 'waiting', function(err) {
            if (err) {
                console.error('DB Error creating game:', err);
                reject(err);
            } else {
                console.log(`DB: Game ${roomCode} created with ID ${this.lastID}`);
                resolve(this.lastID);
            }
        });
        stmt.finalize();
    });
}


function logPlayerJoined(socketId, nickname, gameId) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`INSERT INTO players (socket_id, nickname, game_id, joined_at) VALUES (?, ?, ?, ?)`);
        stmt.run(socketId, nickname, gameId, new Date().toISOString(), function(err) {
            if (err) {
                console.error('DB Error adding player:', err);
                reject(err);
            } else {
                console.log(`DB: Player ${nickname} (ID ${this.lastID}) joined game ${gameId}`);
                resolve(this.lastID);
            }
        });
        stmt.finalize();
    });
}

function logRoundStarted(gameId, roundNumber, category) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`INSERT INTO rounds (game_id, round_number, category, started_at) VALUES (?, ?, ?, ?)`);
        stmt.run(gameId, roundNumber, category, new Date().toISOString(), function(err) {
            if (err) {
                console.error('DB Error starting round:', err);
                reject(err);
            } else {
                console.log(`DB: Round ${roundNumber} started in game ${gameId} with category "${category}"`);
                resolve(this.lastID);
            }
        });
        stmt.finalize();
    });
}

function logSubmission(roundId, playerId, exemplar) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`INSERT INTO submissions (round_id, player_id, exemplar, submitted_at) VALUES (?, ?, ?, ?)`);
        stmt.run(roundId, playerId, exemplar, new Date().toISOString(), function(err) {
            if (err) {
                console.error('DB Error logging submission:', err);
                reject(err);
            } else {
                console.log(`DB: Submission logged for player ${playerId} in round ${roundId}`);
                resolve(this.lastID);
            }
        });
        stmt.finalize();
    });
}

function logVote(submissionId, voterPlayerId, vote) {
    return new Promise((resolve, reject) => {
        const voteValue = vote ? 1 : 0;
        const stmt = db.prepare(`INSERT INTO votes (submission_id, voter_player_id, vote, voted_at) VALUES (?, ?, ?, ?)`);
        stmt.run(submissionId, voterPlayerId, voteValue, new Date().toISOString(), function(err) {
            if (err) {
                console.error('DB Error logging vote:', err);
                reject(err);
            } else {
                console.log(`DB: Vote logged - submission ${submissionId}, voter ${voterPlayerId}, vote ${voteValue}`);
                resolve(this.lastID);
            }
        });
        stmt.finalize();
    });
}

function updateGameStatus(roomCode, status, totalRounds = null) {
    return new Promise((resolve, reject) => {
        if (status === 'completed') {
            const stmt = db.prepare(`UPDATE games SET ended_at = ?, status = ?, total_rounds = ? WHERE room_code = ?`);
            stmt.run(new Date().toISOString(), status, totalRounds, roomCode, function(err) {
                if (err) {
                    console.error('DB Error updating game status:', err);
                    reject(err);
                } else {
                    console.log(`DB: Game ${roomCode} status updated to ${status}`);
                    resolve();
                }
            });
            stmt.finalize();
        }
    });
}

function updatePlayerFinalScore(socketId, gameId, score) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`UPDATE players SET final_score = ? WHERE socket_id = ? AND game_id = ?`);
        stmt.run(score, socketId, gameId, function(err) {
            if (err) {
                console.error('DB Error updating player score:', err);
                reject(err);
            } else {
                console.log(`DB: Player ${socketId} final score updated to ${score}`);
                resolve();
            }
        });
        stmt.finalize();
    });
}

function logPlayerLeft(socketId, gameId) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`UPDATE players SET left_at = ? WHERE socket_id = ? AND game_id = ?`);
        stmt.run(new Date().toISOString(), socketId, gameId, function(err) {
            if (err) {
                console.error('DB Error logging player departure:', err);
                reject(err);
            } else {
                console.log(`DB: Player ${socketId} left game ${gameId}`);
                resolve();
            }
        });
        stmt.finalize();
    });
}


function updateSubmissionResults(points, yesCount, noCount, roundId, playerId) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`UPDATE submissions SET points_earned = ?, yes_votes = ?, no_votes = ? WHERE round_id = ? AND player_id = ?`);
        stmt.run(points, yesCount, noCount, roundId, playerId, function(err) {
            if (err) {
                console.error('DB Error updating submission results:', err);
                reject(err);
            } else {
                console.log(`DB: Submission results updated for player ${playerId} in round ${roundId}`);
                resolve();
            }
        });
        stmt.finalize();
    });
}

// Preset categories for research consistency
const PRESET_CATEGORIES = [
    'furniture', 'tools', 'games', 'clothing', 'vehicles', 'food', 
    'animals', 'colors', 'sports', 'music', 'technology', 'books',
    'drinks', 'toys', 'plants', 'weather', 'emotions', 'professions'
];

// Database function to log category submission
function logCategorySubmission(gameId, playerId, categoryText, isPreset = false) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`INSERT INTO categories (game_id, submitted_by_player_id, category_text, is_preset, submitted_at) VALUES (?, ?, ?, ?, ?)`);
        stmt.run(gameId, playerId, categoryText, isPreset ? 1 : 0, new Date().toISOString(), function(err) {
            if (err) {
                console.error('DB Error logging category submission:', err);
                reject(err);
            } else {
                console.log(`DB: Category "${categoryText}" submitted for game ${gameId}`);
                resolve(this.lastID);
            }
        });
        stmt.finalize();
    });
}

// Mark category as used
function markCategoryAsUsed(gameId, categoryText) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`UPDATE categories SET was_used = 1 WHERE game_id = ? AND category_text = ?`);
        stmt.run(gameId, categoryText, function(err) {
            if (err) {
                console.error('DB Error marking category as used:', err);
                reject(err);
            } else {
                console.log(`DB: Category "${categoryText}" marked as used in game ${gameId}`);
                resolve();
            }
        });
        stmt.finalize();
    });
}

// Get available categories for a game
function getAvailableCategories(gameId) {
    return new Promise((resolve, reject) => {
        // Get player submissions first, then presets
        const stmt = db.prepare(`
            SELECT category_text FROM categories 
            WHERE game_id = ? AND was_used = 0 
            ORDER BY is_preset ASC, submitted_at ASC
        `);
        stmt.all(gameId, (err, rows) => {
            if (err) {
                console.error('DB Error getting categories:', err);
                reject(err);
            } else {
                resolve(rows.map(row => row.category_text));
            }
        });
        stmt.finalize();
    });
}

// Initialize preset categories for a new game
async function initializePresetCategories(gameId) {
    const promises = PRESET_CATEGORIES.map(category => 
        logCategorySubmission(gameId, null, category, true)
    );
    await Promise.all(promises);
    console.log(`Initialized ${PRESET_CATEGORIES.length} preset categories for game ${gameId}`);
}

// Socket connection handling
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    socket.on('create-room', async () => {
        let roomCode;
        do {
            roomCode = generateRoomCode();
        } while (rooms.has(roomCode));

        const room = createRoom(roomCode);
        room.gmSocketId = socket.id;

        room.dbGameId = await logGameCreated(roomCode, socket.id);
        
        // Initialize preset categories immediately
        await initializePresetCategories(room.dbGameId);

        rooms.set(roomCode, room);
        
        socket.join(roomCode);
        socket.emit('room-created', { roomCode });
        
        console.log(`Room ${roomCode} created by host ${socket.id}`);
    });
    
    // Player joins room
    socket.on('join-room', async (data) => {
        const { roomCode, nickname } = data;
        const room = rooms.get(roomCode);

        if (!room) {
            socket.emit('join-error', { message: 'Room not found' });
            return;
        }

        if (!nickname || nickname.trim().length === 0) {
            socket.emit('join-error', { message: 'Nickname required' });
            return;
        }

        // Check if nickname is already taken
        const existingNicknames = Array.from(room.players.values()).map(p => p.nickname.toLowerCase());
        if (existingNicknames.includes(nickname.toLowerCase())) {
            socket.emit('join-error', { message: 'Nickname already taken' });
            return;
        }

        // Add player to room
        const playerId = socket.id;
        const dbPlayerId = await logPlayerJoined(socket.id, nickname.trim(), room.dbGameId);
        room.players.set(playerId, {
            nickname: nickname.trim(),
            score: 0,
            socketId: socket.id,
            hasSubmitted: false,
            hasVoted: false,
            dbPlayerId: dbPlayerId
        });

        socket.join(roomCode);
        socket.emit('join-success', { roomCode, playerId });

        // Notify everyone in the room about the new player
        const playerList = Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted
        }));

        // Send current game state to the newly joined player
        const gameStateData = {
            gameState: room.gameState,
            round: room.round,
            players: playerList
        };

        if (room.gameState === 'lobby') {
            gameStateData.categorySubmissions = room.categorySubmissions || [];
        }

        socket.emit('game-state-update', gameStateData);

        io.to(roomCode).emit('room-update', {
            playerCount: room.players.size,
            players: playerList,
            gameState: room.gameState
        });

        // Send player join update to display
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('player-joined', { 
                nickname: nickname.trim() 
            });
            
            // Also send updated player list to display
            io.to(room.displaySocketId).emit('players-update', { 
                players: playerList 
            });
        }

        console.log(`Player ${nickname} joined room ${roomCode} (lobby phase)`);
    });

    socket.on('submit-category', async (data) => {
        const { category } = data;
        const room = findRoomBySocket(socket.id);
        
        if (!room || !room.players.has(socket.id)) {
            socket.emit('error', { message: 'Not in a room' });
            return;
        }
        
        if (room.gameState !== 'lobby') {
            socket.emit('error', { message: 'Not in lobby phase' });
            return;
        }
        
        if (!category || category.trim().length === 0) {
            socket.emit('error', { message: 'Category cannot be empty' });
            return;
        }
        
        if (category.trim().length > 50) {
            socket.emit('error', { message: 'Category too long (max 50 characters)' });
            return;
        }
        
        const player = room.players.get(socket.id);
        const cleanCategory = category.trim().toLowerCase();
        
        // Log to database for research purposes
        await logCategorySubmission(room.dbGameId, player.dbPlayerId, cleanCategory, false);
        
        // Add to room's category submissions for display
        room.categorySubmissions.push({
            playerId: socket.id,
            nickname: player.nickname,
            category: cleanCategory
        });
        
        // Send confirmation to submitter
        socket.emit('category-submitted', { category: cleanCategory });
        
        // Broadcast updated category list to display
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('categories-update', {
                categorySubmissions: room.categorySubmissions
            });
        }
        
        console.log(`Player ${player.nickname} submitted category "${cleanCategory}" in room ${room.code}`);
    });

    // Host adds category via display interface
    socket.on('host-add-category', async (data) => {
        const { category } = data;
        const room = findRoomBySocket(socket.id);
        
        if (!room || room.displaySocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized - display only' });
            return;
        }
        
        if (!category || category.trim().length === 0) {
            socket.emit('error', { message: 'Category cannot be empty' });
            return;
        }
        
        const cleanCategory = category.trim().toLowerCase();
        
        // Log as host-submitted category
        await logCategorySubmission(room.dbGameId, null, cleanCategory, false);
        
        // Add to room's submissions for display
        room.categorySubmissions.push({
            playerId: 'host',
            nickname: 'Host',
            category: cleanCategory
        });
        
        // Send confirmation
        socket.emit('category-added', { category: cleanCategory });
        
        // Update display
        socket.emit('categories-update', {
            categorySubmissions: room.categorySubmissions
        });
        
        console.log(`Host added category "${cleanCategory}" in room ${room.code}`);
    });

    // Start game from lobby phase
    socket.on('start-lobby-game', async (data) => {
        const room = findRoomBySocket(socket.id);
        
        // Allow either GM or Display to start the game
        if (!room || (room.gmSocketId !== socket.id && room.displaySocketId !== socket.id)) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }
        
        if (room.players.size < 2) {
            socket.emit('error', { message: 'Need at least 2 players to start' });
            return;
        }
        
        // Start first round
        room.round = 1;
        const firstCategory = await selectNextCategory(room);
        
        if (firstCategory) {
            room.currentRoundDbId = await logRoundStarted(room.dbGameId, room.round, firstCategory);
            startSubmissionPhase(room, firstCategory);
            
            // Notify all clients game has started
            const gameStateData = {
                gameState: room.gameState,
                currentCategory: firstCategory,
                round: room.round,
                players: Array.from(room.players.values()).map(p => ({
                    nickname: p.nickname,
                    score: p.score,
                    hasSubmitted: p.hasSubmitted,
                    hasVoted: p.hasVoted
                }))
            };
            
            io.to(room.code).emit('game-started', gameStateData);
            io.to(room.code).emit('game-state-update', gameStateData);
            
            console.log(`Game started in room ${room.code} with category "${firstCategory}"`);
        } else {
            socket.emit('error', { message: 'Unable to start game - no categories available' });
        }
    });

    // Display connects to room
    socket.on('join-display', (data) => {
        const { roomCode } = data;
        const room = rooms.get(roomCode);

        if (!room) {
            socket.emit('error', { message: 'Room not found' });
            return;
        }

        room.displaySocketId = socket.id;
        socket.join(roomCode);
        socket.emit('display-connected', { roomCode });
        
        // Send current game state to display
        const displayData = {
            gameState: room.gameState,
            round: room.round,
            totalPlayers: room.players.size,
            categorySubmissions: room.categorySubmissions || []
        };

        // Include player list for lobby/waiting screen
        if (room.gameState === 'lobby' || room.gameState === 'waiting') {
            displayData.players = Array.from(room.players.values()).map(p => ({
                nickname: p.nickname,
                score: p.score
            }));
        }

        // Only include category and counts if game is actually in progress
        if (room.gameState === 'submitting') {
            displayData.currentCategory = room.currentCategory;
            displayData.submittedCount = Array.from(room.players.values()).filter(p => p.hasSubmitted).length;
        } else if (room.gameState === 'voting') {
            displayData.currentCategory = room.currentCategory;  // <- add this
            displayData.votedCount = Array.from(room.players.values())
                .filter(p => p.hasVoted).length;
            }

        socket.emit('display-update', displayData);

        console.log(`Display connected to room ${roomCode} (${room.gameState} phase)`);
    });

    // GM sets category
    socket.on('set-category', async (data) => {
        const room = findRoomBySocket(socket.id);
        if (!room || room.gmSocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }

        const { category } = data;
        room.currentRoundDbId = await logRoundStarted(room.dbGameId, room.round, category);
        
        // Start timed submission phase
        startSubmissionPhase(room, category);
        
        console.log(`Category "${category}" set for room ${room.code}, timer started`);
    });

    // Player submits exemplar
    socket.on('submit-exemplar', async (data) => {
        const room = findRoomBySocket(socket.id);
        if (!room || !room.players.has(socket.id)) {
            socket.emit('error', { message: 'Not in a room' });
            return;
        }

        if (room.gameState !== 'submitting') {
            socket.emit('error', { message: 'Not in submission phase' });
            return;
        }

        const { exemplar } = data;
        const player = room.players.get(socket.id);
        
        const submissionDbId = await logSubmission(room.currentRoundDbId, player.dbPlayerId, exemplar.trim());

        if (player.hasSubmitted) {
            socket.emit('error', { message: 'Already submitted' });
            return;
        }

        // Add submission
        room.submissions.push({
            playerId: socket.id,
            nickname: player.nickname,
            exemplar: exemplar.trim(),
            votes: new Map(),
            dbSubmissionId: submissionDbId
        });

        player.hasSubmitted = true;

        // Send confirmation to the submitting player
        socket.emit('submission-confirmed', { exemplar: exemplar.trim() });

        const gameStateData = {
            gameState: room.gameState,
            currentCategory: room.currentCategory,
            round: room.round,
            players: Array.from(room.players.values()).map(p => ({
                nickname: p.nickname,
                score: p.score,
                hasSubmitted: p.hasSubmitted,
                hasVoted: p.hasVoted
            }))
        };

        // Send update to GM and display, but not to all players
        if (room.gmSocketId) {
            io.to(room.gmSocketId).emit('game-state-update', gameStateData);
        }
        
        // Update display with submission count
        if (room.displaySocketId) {
            const submittedCount = Array.from(room.players.values()).filter(p => p.hasSubmitted).length;
            io.to(room.displaySocketId).emit('display-update', {
                gameState: room.gameState,
                currentCategory: room.currentCategory,
                round: room.round,
                totalPlayers: room.players.size,
                submittedCount: submittedCount
            });
        }
        checkSubmissionComplete(room);
        console.log(`Player ${player.nickname} submitted "${exemplar}" in room ${room.code}`);
    });


    socket.on('submit-votes', async (data) => {
        const room = findRoomBySocket(socket.id);
        if (!room || !room.players.has(socket.id)) {
            socket.emit('error', { message: 'Not in a room' });
            return;
        }

        if (room.gameState !== 'voting') {
            socket.emit('error', { message: 'Not in voting phase' });
            return;
        }

        const { votes } = data; // votes is { exemplarIndex: boolean }

        // ALWAYS get player before using it
        const player = room.players.get(socket.id);
        if (!player) {
            socket.emit('error', { message: 'Player not found in room' });
            return;
        }

        if (player.hasVoted) {
            socket.emit('error', { message: 'Already voted' });
            return;
        }

        // Record votes + DB logging
        for (const [exemplarIndex, vote] of Object.entries(votes || {})) {
            const index = parseInt(exemplarIndex, 10);
            if (Number.isInteger(index) && room.submissions[index]) {
            room.submissions[index].votes.set(socket.id, !!vote);
            await logVote(room.submissions[index].dbSubmissionId, player.dbPlayerId, !!vote);
            }
        }

        // Mark as voted BEFORE computing counts
        player.hasVoted = true;

        const votedCount = Array.from(room.players.values()).filter(p => p.hasVoted).length;
        const allVoted = (votedCount === room.players.size && room.players.size > 0);

        if (allVoted) {
            // Show final  N/N  on host
            if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
                gameState: 'voting',
                currentCategory: room.currentCategory,
                totalPlayers: room.players.size,
                votedCount
            });
            }

            // Stop the timer and move to results
            if (room.currentTimer) room.currentTimer.cancel();

            // Optional tiny delay so the 3/3 paints before switching screens
            setTimeout(() => startResultsPhase(room), 150);
            return; // IMPORTANT: no further 'voting' updates
        }

        // Not all voted yet: broadcast normal updates
        const gameStateData = {
            gameState: room.gameState, // 'voting'
            players: Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted
            }))
        };
        io.to(room.code).emit('game-state-update', gameStateData);

        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
            gameState: room.gameState, // 'voting'
            currentCategory: room.currentCategory,
            totalPlayers: room.players.size,
            votedCount
            });
        }

        console.log(`Player ${player.nickname} voted in room ${room.code} (${votedCount}/${room.players.size})`);
        });




    // GM ends game
    socket.on('end-game', async () => {
        const room = findRoomBySocket(socket.id);
        if (!room || room.gmSocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }

        room.gameState = 'ended';
        await updateGameStatus(room.code, 'completed', room.round);

        const finalScores = Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score
        })).sort((a, b) => b.score - a.score);

        // Send to all players
        io.to(room.code).emit('game-ended', { finalScores });
        
        // Show final scoreboard on display (using the existing scoreboard structure)
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('show-round-scoreboard', {
                players: finalScores,
                round: room.round,
                isGameWide: true,
                isFinal: true  // Flag to indicate this is the final game scoreboard
            });
        }

        console.log(`Game ended in room ${room.code}`);
    });

// Handle disconnections
    socket.on('disconnect', async () => {
        console.log('Client disconnected:', socket.id);
        
        // Find and clean up from any rooms
        for (const [roomCode, room] of rooms.entries()) {
            if (room.gmSocketId === socket.id) {
                // GM disconnected - notify players and clean up room
                io.to(roomCode).emit('gm-disconnected');
                rooms.delete(roomCode);
                console.log(`Room ${roomCode} deleted - GM disconnected`);
            } else if (room.displaySocketId === socket.id) {
                // Display disconnected
                room.displaySocketId = null;
                console.log(`Display disconnected from room ${roomCode}`);
            } else if (room.players.has(socket.id)) {
                // Player disconnected
                const player = room.players.get(socket.id);

                await logPlayerLeft(socket.id, room.dbGameId);

                room.players.delete(socket.id);
                
                const playerList = Array.from(room.players.values()).map(p => ({
                    nickname: p.nickname,
                    score: p.score,
                    hasSubmitted: p.hasSubmitted,
                    hasVoted: p.hasVoted
                }));

                io.to(roomCode).emit('room-update', {
                    playerCount: room.players.size,
                    players: playerList,
                    gameState: room.gameState
                });

                // Send player leave update to display
                if (room.displaySocketId && player) {
                    io.to(room.displaySocketId).emit('player-left', { 
                        nickname: player.nickname 
                    });
                    
                    // Also send updated player list to display
                    io.to(room.displaySocketId).emit('players-update', { 
                        players: playerList 
                    });
                }

                console.log(`Player ${player?.nickname} left room ${roomCode}`);
            }
            
            // Cancel any active timers for this room (happens regardless of who disconnected)
            if (room.currentTimer) {
                room.currentTimer.cancel();
            }
        }
    });
});

// Helper function to find room by socket ID
function findRoomBySocket(socketId) {
    for (const room of rooms.values()) {
        if (room.gmSocketId === socketId || room.players.has(socketId)) {
            return room;
        }
    }
    return null;
}

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Open http://localhost:${PORT} to start`);
});

app.get('/export/csv', (req, res) => {
    try {
        const { password } = req.query;
        
        if (password !== 'research123') {
            return res.status(401).json({ error: 'Unauthorized' });
        }

        const query = `
        SELECT 
            g.room_code,
            g.created_at as game_start,
            g.ended_at as game_end,
            r.round_number,
            r.category,
            p.nickname,
            s.exemplar,
            s.points_earned,
            s.yes_votes,
            s.no_votes,
            v.vote as voter_choice,
            voter.nickname as voter_name
        FROM games g
        JOIN rounds r ON g.id = r.game_id
        JOIN submissions s ON r.id = s.round_id
        JOIN players p ON s.player_id = p.id
        LEFT JOIN votes v ON s.id = v.submission_id
        LEFT JOIN players voter ON v.voter_player_id = voter.id
        ORDER BY g.created_at, r.round_number, s.exemplar, voter.nickname
        `;
        
        db.all(query, [], (err, data) => {
            if (err) {
                return res.status(500).json({ error: err.message });
            }
            
            const csv = [
                'room_code,game_start,game_end,round_number,category,submitter,exemplar,points_earned,yes_votes,no_votes,voter_choice,voter_name',
                ...data.map(row => Object.values(row).map(val => 
                    val === null ? '' : `"${val}"`
                ).join(','))
            ].join('\n');
            
            res.setHeader('Content-Type', 'text/csv');
            res.setHeader('Content-Disposition', 'attachment; filename="category_game_data.csv"');
            res.send(csv);
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});