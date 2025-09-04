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
        players: new Map(), // playerId -> {nickname, score, socketId, hasSubmitted, hasVoted}
        gmSocketId: null,
        displaySocketId: null,
        gameState: 'waiting', // waiting, waiting-for-category, submitting, voting, results, ended
        currentCategory: '',
        submissions: [], // {playerId, nickname, exemplar, votes: {playerId: boolean}}
        round: 0,
        createdAt: new Date()
    };
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


// Socket connection handling
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    // GM creates a new room
    socket.on('create-room', async () => {
        let roomCode;
        do {
            roomCode = generateRoomCode();
        } while (rooms.has(roomCode));

        const room = createRoom(roomCode);
        room.gmSocketId = socket.id;

        room.dbGameId = await logGameCreated(roomCode, socket.id);

        rooms.set(roomCode, room);
        
        socket.join(roomCode);
        socket.emit('room-created', { roomCode });
        
        console.log(`Room ${roomCode} created by GM ${socket.id}`);
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

        console.log(`Player ${nickname} joined room ${roomCode}`);
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
            totalPlayers: room.players.size
        };

        // Include player list for waiting screen
        if (room.gameState === 'waiting') {
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
            displayData.votedCount = Array.from(room.players.values()).filter(p => p.hasVoted).length;
        }

        socket.emit('display-update', displayData);

        console.log(`Display connected to room ${roomCode}`);
    });

    // GM starts the game (placeholder for now)
    socket.on('start-game', () => {
        const room = findRoomBySocket(socket.id);
        if (!room || room.gmSocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }

        room.gameState = 'waiting-for-category';
        room.round = 1;
        
        const gameStateData = {
            gameState: room.gameState,
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
        
        // Update display
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
                gameState: room.gameState,
                round: room.round,
                totalPlayers: room.players.size
            });
        }

        console.log(`Game started in room ${room.code}`);
    });

    // GM sets category
    socket.on('set-category', async (data) => {
        const room = findRoomBySocket(socket.id);
        if (!room || room.gmSocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }

        const { category } = data;
        room.currentCategory = category;
        room.gameState = 'submitting';

        room.currentRoundDbId = await logRoundStarted(room.dbGameId, room.round, category);

        room.submissions = [];
        
        console.log(`Category set to "${category}" in room ${room.code}`); // Debug log
        
        // Reset player submission status
        room.players.forEach(player => {
            player.hasSubmitted = false;
            player.hasVoted = false;
        });

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

        io.to(room.code).emit('game-state-update', gameStateData);
        
        // Update display
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
                gameState: room.gameState,
                currentCategory: room.currentCategory,
                round: room.round,
                totalPlayers: room.players.size
            });
        }

        console.log(`Category "${category}" set for room ${room.code}, sent to ${room.players.size} players`);
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

        console.log(`Player ${player.nickname} submitted "${exemplar}" in room ${room.code}`);
    });

    // GM starts voting phase
    socket.on('start-voting', () => {
        const room = findRoomBySocket(socket.id);
        if (!room || room.gmSocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }

        room.gameState = 'voting';
        
        // Reset voting status
        room.players.forEach(player => {
            player.hasVoted = false;
        });

        // Clear existing votes
        room.submissions.forEach(submission => {
            submission.votes.clear();
        });

        const gameStateData = {
            gameState: room.gameState,
            currentCategory: room.currentCategory, // Make sure to include current category
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
        
        // Update display with current category
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
                gameState: room.gameState,
                currentCategory: room.currentCategory, // Explicitly pass the current category
                totalPlayers: room.players.size,
                votedCount: 0
            });
        }

        console.log(`Voting started in room ${room.code} for category "${room.currentCategory}"`);
    });

    // Player submits votes
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

        const { votes } = data; // votes is {exemplarIndex: boolean}
        const player = room.players.get(socket.id);
        
        if (player.hasVoted) {
            socket.emit('error', { message: 'Already voted' });
            return;
        }

        // Record votes with database logging
        for (const [exemplarIndex, vote] of Object.entries(votes)) {
            const index = parseInt(exemplarIndex);
            if (room.submissions[index]) {
                room.submissions[index].votes.set(socket.id, vote);
                
                // Log to database
                await logVote(room.submissions[index].dbSubmissionId, player.dbPlayerId, vote);
            }
        }

        player.hasVoted = true;

        const votedCount = Array.from(room.players.values()).filter(p => p.hasVoted).length;

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
        
        // Update display vote counter
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('display-update', {
                gameState: room.gameState,
                totalPlayers: room.players.size,
                votedCount: votedCount
            });
        }

        console.log(`Player ${player.nickname} voted in room ${room.code} (${votedCount}/${room.players.size})`);
    });
// GM shows results
socket.on('show-results', async () => {
    const room = findRoomBySocket(socket.id);
    if (!room || room.gmSocketId !== socket.id) {
        socket.emit('error', { message: 'Not authorized' });
        return;
    }

    room.gameState = 'results';
    
    // Calculate scores and results
    const results = [];
    for (const submission of room.submissions) {
        const votes = Array.from(submission.votes.entries()).map(([playerId, vote]) => ({
            playerId,
            vote
        }));
        
        const yesCount = votes.filter(v => v.vote).length;
        const noCount = votes.length - yesCount;
        const points = Math.min(yesCount, noCount); // Smaller split gets points
        
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

    // Store results in room for GM navigation
    room.currentResults = results;
    room.currentResultIndex = -1; // Start before first result

    const gameStateData = {
        gameState: room.gameState,
        players: Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted
        }))
    };

    // Send to players
    io.to(room.code).emit('game-state-update', gameStateData);
    
    // Send to GM with navigation controls
    socket.emit('results-ready', {
        totalResults: results.length,
        currentIndex: -1
    });

    // Initialize results mode on display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('results-mode-start', {
            totalResults: results.length
        });
    }

    console.log(`Results ready for room ${room.code}, ${results.length} exemplars`);
});

// GM navigates through results
socket.on('show-next-result', () => {
    const room = findRoomBySocket(socket.id);
    if (!room || room.gmSocketId !== socket.id || !room.currentResults) {
        socket.emit('error', { message: 'Not authorized or no results ready' });
        return;
    }

    if (room.currentResultIndex < room.currentResults.length - 1) {
        room.currentResultIndex++;
        const result = room.currentResults[room.currentResultIndex];
        
        // Send to GM
        socket.emit('result-navigation', {
            currentIndex: room.currentResultIndex,
            totalResults: room.currentResults.length,
            canGoNext: room.currentResultIndex < room.currentResults.length - 1,
            canGoPrev: room.currentResultIndex > 0
        });

        // Send to display
        if (room.displaySocketId) {
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
    }
});

socket.on('show-prev-result', () => {
    const room = findRoomBySocket(socket.id);
    if (!room || room.gmSocketId !== socket.id || !room.currentResults) {
        socket.emit('error', { message: 'Not authorized or no results ready' });
        return;
    }

    if (room.currentResultIndex > 0) {
        room.currentResultIndex--;
        const result = room.currentResults[room.currentResultIndex];
        
        // Send to GM
        socket.emit('result-navigation', {
            currentIndex: room.currentResultIndex,
            totalResults: room.currentResults.length,
            canGoNext: room.currentResultIndex < room.currentResults.length - 1,
            canGoPrev: room.currentResultIndex > 0
        });

        // Send to display
        if (room.displaySocketId) {
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
    }
});

// GM shows final summary (top/bottom controversial)
// GM shows final summary (enhanced logic for different numbers of exemplars)
socket.on('show-final-summary', () => {
    const room = findRoomBySocket(socket.id);
    if (!room || room.gmSocketId !== socket.id || !room.currentResults) {
        socket.emit('error', { message: 'Not authorized or no results ready' });
        return;
    }

    // Sort by points (most to least), then by controversy as tiebreaker
    const sortedByPoints = [...room.currentResults].sort((a, b) => {
        if (a.points !== b.points) {
            return b.points - a.points; // Most points first
        }
        // Tiebreaker: more controversial (closer to 50/50) first
        const aControversy = Math.abs(a.yesCount - a.noCount);
        const bControversy = Math.abs(b.yesCount - b.noCount);
        return aControversy - bControversy;
    });

    let displayData;
    
    if (sortedByPoints.length <= 6) {
        // Show all exemplars if 6 or fewer
        displayData = {
            showAll: true,
            allResults: sortedByPoints,
            title: `All ${sortedByPoints.length} Exemplars (Most to Least Points)`
        };
    } else {
        // Show top 3 and bottom 3 if more than 6
        const topResults = sortedByPoints.slice(0, 3);
        const bottomResults = sortedByPoints.slice(-3);
        
        displayData = {
            showAll: false,
            topResults: topResults,
            bottomResults: bottomResults,
            title: 'Top & Bottom Scoring Exemplars'
        };
    }

    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('show-enhanced-summary', displayData);
    }

    socket.emit('summary-shown');
});

// GM shows scoreboard (game-wide, not per round)
socket.on('show-round-scoreboard', () => {
    const room = findRoomBySocket(socket.id);
    if (!room || room.gmSocketId !== socket.id) {
        socket.emit('error', { message: 'Not authorized' });
        return;
    }

    const sortedPlayers = Array.from(room.players.values())
        .map(p => ({ nickname: p.nickname, score: p.score }))
        .sort((a, b) => b.score - a.score);

    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('show-round-scoreboard', {
            players: sortedPlayers,
            round: room.round,
            isGameWide: true  // Flag to indicate this is cumulative scoring
        });
    }

    socket.emit('scoreboard-shown');
});

    // GM starts next round
    socket.on('next-round', () => {
        const room = findRoomBySocket(socket.id);
        if (!room || room.gmSocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized' });
            return;
        }

        room.round++;
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
                round: room.round,
                totalPlayers: room.players.size
            });
        }

        console.log(`Round ${room.round} started in room ${room.code}`);
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