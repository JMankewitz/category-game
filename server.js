const sqlite3 = require('sqlite3').verbose();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Serve static files from public directory
app.use(express.static('public'));

// In-memory storage for rooms
const rooms = new Map();

// Track socket to player mapping for quick lookups
const socketToPlayer = new Map(); // socketId -> {roomCode, playerId}

// Initialize database
const dbPath = path.join(__dirname, 'game_data.db');
const db = new sqlite3.Database(dbPath);

// Enable foreign keys
db.run('PRAGMA foreign_keys = ON');

class GameTimer {
    constructor(duration, onComplete, onTick = null, phase = '') {
        this.duration = duration;
        this.remaining = duration;
        this.onComplete = onComplete;
        this.onTick = onTick;
        this.phase = phase;
        this.interval = null;
        this.isActive = false;
        this.startTime = null;
    }
    
    start() {
        if (this.isActive) return;
        
        this.isActive = true;
        this.startTime = Date.now();
        this.interval = setInterval(() => {
            this.remaining--;
            
            if (this.onTick) {
                this.onTick(this.remaining, this.phase);
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

    // Get current state for reconnection
    getState() {
        return {
            remaining: this.remaining,
            phase: this.phase,
            isActive: this.isActive,
            startTime: this.startTime
        };
    }

    // Restore timer state after reconnection
    restoreState(savedState) {
        if (!savedState.isActive) return;
        
        const elapsed = Math.floor((Date.now() - savedState.startTime) / 1000);
        this.remaining = Math.max(0, savedState.remaining - elapsed);
        this.phase = savedState.phase;
        
        if (this.remaining > 0) {
            this.start();
        } else {
            this.complete();
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
        players: new Map(), // playerId -> player object
        gmSocketId: null,
        displaySocketId: null,
        gameState: 'lobby',
        currentCategory: '',
        submissions: [],
        round: 0,
        createdAt: new Date(),
        
        // Category management
        categorySubmissions: [], // Array of {playerId, nickname, category}
        availableCategories: [], // Cached list of available categories
        
        // Timer properties
        currentTimer: null,
        timerState: null, // Saved timer state for reconnections
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

// Enhanced player lookup functions
function findPlayerBySocketId(socketId) {
    const mapping = socketToPlayer.get(socketId);
    if (!mapping) return null;
    
    const room = rooms.get(mapping.roomCode);
    if (!room) return null;
    
    return room.players.get(mapping.playerId);
}

function findRoomBySocketId(socketId) {
    // First check if it's a player socket
    const mapping = socketToPlayer.get(socketId);
    if (mapping) {
        return rooms.get(mapping.roomCode);
    }
    
    // If not a player, check if it's a GM or display socket
    for (const room of rooms.values()) {
        if (room.gmSocketId === socketId || room.displaySocketId === socketId) {
            return room;
        }
    }
    
    return null;
}

function updateSocketMapping(socketId, roomCode, playerId) {
    socketToPlayer.set(socketId, { roomCode, playerId });
}

function removeSocketMapping(socketId) {
    socketToPlayer.delete(socketId);
}

function broadcastTimerUpdate(room, remaining, phase) {
    const timerData = {
        remaining: remaining,
        phase: phase,
        gameState: room.gameState
    };

    // Save timer state for reconnections
    room.timerState = {
        remaining,
        phase,
        gameState: room.gameState,
        timestamp: Date.now()
    };
    
    // Send to all connected players
    room.players.forEach(player => {
        if (player.isConnected && player.socketId) {
            io.to(player.socketId).emit('timer-update', timerData);
        }
    });
    
    // Send to display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('timer-update', timerData);
    }
}

function broadcastGameState(room, excludeSocketId = null) {
    const gameStateData = {
        gameState: room.gameState,
        currentCategory: room.currentCategory,
        round: room.round,
        players: Array.from(room.players.values()).map(p => ({
            nickname: p.nickname,
            score: p.score,
            hasSubmitted: p.hasSubmitted,
            hasVoted: p.hasVoted,
            isConnected: p.isConnected
        }))
    };

    // Add timer data if active
    if (room.timerState) {
        gameStateData.timerRemaining = room.timerState.remaining;
        gameStateData.timerPhase = room.timerState.phase;
    }

    // Add phase-specific data
    if (room.gameState === 'lobby') {
        gameStateData.categorySubmissions = room.categorySubmissions || [];
    } else if (room.gameState === 'voting') {
        gameStateData.submissions = room.submissions.map(s => ({
            exemplar: s.exemplar,
            submittedBy: s.nickname
        }));
    }

    // Send to all connected players (except excluded socket)
    room.players.forEach(player => {
        if (player.isConnected && player.socketId && player.socketId !== excludeSocketId) {
            io.to(player.socketId).emit('game-state-update', gameStateData);
        }
    });

    // Update display
    updateDisplay(room);
}

// Enhanced display update
function updateDisplay(room) {
    if (!room.displaySocketId) return;

    const displayData = {
        gameState: room.gameState,
        round: room.round,
        totalPlayers: room.players.size,
        categorySubmissions: room.categorySubmissions || []
    };

    // Add connected player list
    displayData.players = Array.from(room.players.values()).map(p => ({
        nickname: p.nickname,
        score: p.score,
        isConnected: p.isConnected
    }));

    // Add phase-specific data
    if (room.gameState === 'submitting') {
        displayData.currentCategory = room.currentCategory;
        displayData.submittedCount = Array.from(room.players.values())
            .filter(p => p.hasSubmitted).length;
    } else if (room.gameState === 'voting') {
        displayData.currentCategory = room.currentCategory;
        displayData.votedCount = Array.from(room.players.values())
            .filter(p => p.hasVoted).length;
    }

    io.to(room.displaySocketId).emit('display-update', displayData);
}

// Database helper to get player by ID
function getPlayerFromDB(playerId, gameId) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`
            SELECT * FROM players 
            WHERE player_id = ? AND game_id = ?
        `);
        stmt.get(playerId, gameId, (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
        stmt.finalize();
    });
}

// Enhanced reconnection function
async function handlePlayerReconnection(socket, roomCode, playerId) {
    const room = rooms.get(roomCode);
    if (!room) {
        socket.emit('reconnect-error', { message: 'Room not found' });
        return;
    }

    let player = room.players.get(playerId);
    
    // If player not in memory, try to restore from database
    if (!player) {
        try {
            const dbPlayer = await getPlayerFromDB(playerId, room.dbGameId);
            if (!dbPlayer) {
                socket.emit('reconnect-error', { message: 'Player not found' });
                return;
            }
            
            // Restore player to memory
            player = {
                playerId: dbPlayer.player_id,
                dbPlayerId: dbPlayer.id,
                socketId: socket.id,
                nickname: dbPlayer.nickname,
                score: dbPlayer.final_score || 0,
                hasSubmitted: false, // Will be determined by game state
                hasVoted: false,     // Will be determined by game state
                isConnected: true
            };
            
            room.players.set(playerId, player);
        } catch (error) {
            console.error('Error restoring player from DB:', error);
            socket.emit('reconnect-error', { message: 'Failed to restore player data' });
            return;
        }
    } else {
        // Player exists in memory, just reconnect
        player.socketId = socket.id;
        player.isConnected = true;
    }

    // Update socket mapping
    updateSocketMapping(socket.id, roomCode, playerId);
    socket.join(roomCode);

    // Update database
    try {
        await new Promise((resolve, reject) => {
            const stmt = db.prepare(`UPDATE players SET socket_id = ?, is_connected = 1 WHERE player_id = ?`);
            stmt.run(socket.id, playerId, err => err ? reject(err) : resolve());
            stmt.finalize();
        });
    } catch (error) {
        console.error('DB reconnect update error:', error);
    }

    // Send reconnection success with full game state
    const reconnectData = {
        roomCode,
        playerId,
        nickname: player.nickname,
        score: player.score,
        gameState: room.gameState,
        round: room.round
    };

    // Add timer state if active
    if (room.timerState) {
        const elapsed = Math.floor((Date.now() - room.timerState.timestamp) / 1000);
        const remaining = Math.max(0, room.timerState.remaining - elapsed);
        
        if (remaining > 0) {
            reconnectData.timerRemaining = remaining;
            reconnectData.timerPhase = room.timerState.phase;
            
            // Send timer update
            setTimeout(() => {
                socket.emit('timer-update', {
                    remaining,
                    phase: room.timerState.phase,
                    gameState: room.gameState
                });
            }, 100);
        }
    }

    // Add phase-specific data
    if (room.gameState === 'voting' && room.submissions.length > 0) {
        reconnectData.submissions = room.submissions.map(s => ({
            exemplar: s.exemplar,
            submittedBy: s.nickname
        }));
    }

    socket.emit('reconnect-success', reconnectData);

    // Broadcast to others that player reconnected
    room.players.forEach(p => {
        if (p.isConnected && p.socketId && p.socketId !== socket.id) {
            io.to(p.socketId).emit('player-reconnected', { nickname: player.nickname });
        }
    });

    // Update display
    if (room.displaySocketId) {
        io.to(room.displaySocketId).emit('player-reconnected', { nickname: player.nickname });
        updateDisplay(room);
    }

    // Send current game state
    broadcastGameState(room, socket.id);
    setTimeout(() => {
        socket.emit('game-state-update', {
            gameState: room.gameState,
            currentCategory: room.currentCategory,
            round: room.round,
            players: Array.from(room.players.values()).map(p => ({
                nickname: p.nickname,
                score: p.score,
                hasSubmitted: p.hasSubmitted,
                hasVoted: p.hasVoted,
                isConnected: p.isConnected
            })),
            ...(room.gameState === 'voting' && room.submissions.length > 0 ? {
                submissions: room.submissions.map(s => ({
                    exemplar: s.exemplar,
                    submittedBy: s.nickname
                }))
            } : {})
        });
    }, 100);

    console.log(`Player ${player.nickname} successfully reconnected to room ${roomCode}`);
}

async function selectNextCategory(room) {
    try {
        const available = await getAvailableCategories(room.dbGameId);
        if (available.length === 0) return null;

        const playerPool = available.filter(c => !c.isPreset);
        const pool = playerPool.length ? playerPool : available;  // prefer players
        const selected = pool[Math.floor(Math.random() * pool.length)]; // random within the chosen pool

        await markCategoryAsUsed(room.dbGameId, selected.text);
        return selected.text;
    } catch (error) {
        console.error('Error selecting category:', error);
        return null;
    }
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
        },
        'submission'
    );
    
    room.currentTimer.start();
    
    // Broadcast game state
    try {
        broadcastGameState(room);
    } catch (error) {
        console.error('Error broadcasting game state:', error);
    }

    console.log(`Submission phase started for room ${room.code} with category "${category}"`);
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
        },
        'voting'
    );
    
    room.currentTimer.start();

    try {
        broadcastGameState(room);
    } catch (error) {
        console.error('Error broadcasting game state:', error);
    }

    console.log(`Voting phase started for room ${room.code}`);

}

// Check if all players have voted (early completion)
function checkVotingComplete(room) {
    const votedCount = Array.from(room.players.values())
        .filter(p => p.hasVoted).length;

    if (votedCount === room.players.size && room.players.size > 0) {
        console.log(`All players voted early in room ${room.code}`);

        if (room.currentTimer) {
            room.currentTimer.cancel();
        }

        startResultsPhase(room);
        return true;
    }
    return false;
}

// Updated submission handler
async function handleSubmitExemplar(socket, data) {
    const mapping = socketToPlayer.get(socket.id);
    if (!mapping) {
        socket.emit('error', { message: 'Not in a room' });
        return;
    }

    const room = rooms.get(mapping.roomCode);
    const player = room.players.get(mapping.playerId);
    
    if (!room || !player) {
        socket.emit('error', { message: 'Player not found' });
        return;
    }

    if (room.gameState !== 'submitting') {
        socket.emit('error', { message: 'Not in submission phase' });
        return;
    }

    if (player.hasSubmitted) {
        socket.emit('error', { message: 'Already submitted' });
        return;
    }

    const { exemplar } = data;
    
    // Log to database
    const submissionDbId = await logSubmission(room.currentRoundDbId, player.dbPlayerId, exemplar.trim());

    // Add submission
    room.submissions.push({
        playerId: mapping.playerId, // Use playerId instead of socketId
        nickname: player.nickname,
        exemplar: exemplar.trim(),
        votes: new Map(),
        dbSubmissionId: submissionDbId
    });

    player.hasSubmitted = true;

    // Send confirmation to submitting player only
    socket.emit('submission-confirmed', { exemplar: exemplar.trim() });

    // NEW WAY: Single broadcast to update all clients
    try {
        broadcastGameState(room);
    } catch (error) {
        console.error('Error broadcasting game state:', error);
    }
    
    // Check for early completion
    checkSubmissionComplete(room);
    
    console.log(`Player ${player.nickname} submitted "${exemplar}" in room ${room.code}`);
}

// Updated voting handler
async function handleSubmitVotes(socket, data) {
    const mapping = socketToPlayer.get(socket.id);
    if (!mapping) {
        socket.emit('error', { message: 'Not in a room' });
        return;
    }

    const room = rooms.get(mapping.roomCode);
    const player = room.players.get(mapping.playerId);
    
    if (!room || !player) {
        socket.emit('error', { message: 'Player not found' });
        return;
    }

    if (room.gameState !== 'voting') {
        socket.emit('error', { message: 'Not in voting phase' });
        return;
    }

    if (player.hasVoted) {
        socket.emit('error', { message: 'Already voted' });
        return;
    }

    const { votes } = data;

    // Record votes and log to database
    for (const [exemplarIndex, vote] of Object.entries(votes || {})) {
        const index = parseInt(exemplarIndex, 10);
        if (Number.isInteger(index) && room.submissions[index]) {
            // Use playerId instead of socketId for vote tracking
            room.submissions[index].votes.set(mapping.playerId, !!vote);
            await logVote(room.submissions[index].dbSubmissionId, player.dbPlayerId, !!vote);
        }
    }

    player.hasVoted = true;

    // NEW WAY: Single broadcast updates everyone
    try {
        broadcastGameState(room);
    } catch (error) {
        console.error('Error broadcasting game state:', error);
    }
    
    // Check for early completion
    checkVotingComplete(room);
    
    console.log(`Player ${player.nickname} voted in room ${room.code}`);
}

// Updated early completion check
function checkSubmissionComplete(room) {
    const submittedCount = Array.from(room.players.values())
        .filter(p => p.hasSubmitted).length;
    
    if (submittedCount === room.players.size && room.players.size > 0) {
        console.log(`All players submitted early in room ${room.code}`);
        
        if (room.currentTimer) {
            room.currentTimer.cancel();
        }
        
        startVotingPhase(room);
        return true;
    }
    return false;
}

// Updated results phase with enhanced scoring
async function startResultsPhase(room) {
    room.gameState = 'results';
    
    if (room.currentTimer) room.currentTimer.cancel();
    
    // Calculate scores and results
    const results = [];
    for (const submission of room.submissions) {
        // Convert playerId-based votes to the format expected by results
        const votes = Array.from(submission.votes.entries()).map(([playerId, vote]) => ({
            playerId,
            vote
        }));
        
        const yesCount = votes.filter(v => v.vote).length;
        const noCount = votes.length - yesCount;
        const points = Math.min(yesCount, noCount);
        
        // Award points to submitter using playerId
        const submitter = room.players.get(submission.playerId);
        if (submitter) {
            submitter.score += points;
            // Update database with new score
            await updatePlayerFinalScore(submission.playerId, room.dbGameId, submitter.score);
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

    // NEW WAY: Single broadcast handles all updates
    try {
        broadcastGameState(room);
    } catch (error) {
        console.error('Error broadcasting game state:', error);
    }
    
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
            player_id TEXT UNIQUE NOT NULL,
            nickname TEXT NOT NULL,
            game_id INTEGER NOT NULL,
            final_score INTEGER DEFAULT 0,
            is_connected INTEGER DEFAULT 1,
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

function logPlayerJoined(playerId, socketId, nickname, gameId) {
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`
      INSERT INTO players (player_id, socket_id, nickname, game_id, is_connected, joined_at)
      VALUES (?, ?, ?, ?, 1, ?)
    `);
    stmt.run(playerId, socketId, nickname, gameId, new Date().toISOString(), function(err) {
      if (err) return reject(err);
      resolve(this.lastID);
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

function updatePlayerFinalScore(playerId, gameId, score) {
    return new Promise((resolve, reject) => {
        const stmt = db.prepare(`UPDATE players SET final_score = ? WHERE player_id = ? AND game_id = ?`);
        stmt.run(score, playerId, gameId, function(err) {
            if (err) {
                console.error('DB Error updating player final score:', err);
                reject(err);
            } else {
                console.log(`DB: Player ${playerId} score updated to ${score} in game ${gameId}`);
                resolve();
            }
        });
        stmt.finalize();
    });
}

function setPlayerConnected(playerId, isConnected) {
  return new Promise((resolve, reject) => {
    const stmt = db.prepare(`UPDATE players SET is_connected = ? WHERE player_id = ?`);
    stmt.run(isConnected ? 1 : 0, playerId, function(err) {
      if (err) return reject(err);
      resolve();
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
            SELECT category_text, is_preset
            FROM categories
            WHERE game_id = ? AND was_used = 0
            ORDER BY is_preset ASC, submitted_at ASC
            `);
        stmt.all(gameId, (err, rows) => {
            if (err) reject(err);
            else resolve(rows.map(r => ({ text: r.category_text, isPreset: !!r.is_preset })));
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

        // Check if nickname already taken
        const existingNicknames = Array.from(room.players.values())
            .filter(p => p.isConnected)
            .map(p => p.nickname.toLowerCase());
        if (existingNicknames.includes(nickname.toLowerCase())) {
            socket.emit('join-error', { message: 'Nickname already taken' });
            return;
        }

        // Create persistent playerId
        const playerId = uuidv4();

        try {
            // Log in DB (include playerId if you update schema)
            const dbPlayerId = await logPlayerJoined(playerId, socket.id, nickname.trim(), room.dbGameId);

            room.players.set(playerId, {
                playerId,
                dbPlayerId,
                socketId: socket.id,
                nickname: nickname.trim(),
                score: 0,
                hasSubmitted: false,
                hasVoted: false,
                isConnected: true
            });

             updateSocketMapping(socket.id, roomCode, playerId);

            socket.join(roomCode);

            socket.emit('join-success', { roomCode, playerId, nickname: nickname.trim() });

            try {
                broadcastGameState(room);
            } catch (error) {
                console.error('Error broadcasting game state:', error);
            }

            if (room.displaySocketId) {
                io.to(room.displaySocketId).emit('player-joined', { nickname: nickname.trim() });
            }


            console.log(`Player ${nickname} joined room ${roomCode} with playerId ${playerId}`);
        } catch (error) {
            console.error('Error creating player:', error);
            socket.emit('join-error', { message: 'Failed to join game' });
        }
    });

    // Enhanced reconnection handler
    socket.on('reconnect-player', async ({ roomCode, playerId }) => {
        await handlePlayerReconnection(socket, roomCode, playerId);
    });

    socket.on('submit-category', async (data) => {
        const { category } = data;
        const mapping = socketToPlayer.get(socket.id);

        if (!mapping) {
            socket.emit('error', { message: 'Not in a room' });
            return;
        }

        const room = rooms.get(mapping.roomCode);
        const player = room.players.get(mapping.playerId);

        if (!room || !player) {
            socket.emit('error', { message: 'Player not found' });
            return;
        }

        if (room.gameState !== 'lobby') {
            socket.emit('error', { message: 'Not in lobby phase' });
            return;
        }
        
        if (!category || category.trim().length === 0 || category.trim().length > 50) {
            socket.emit('error', { message: 'Invalid category' });
            return;
        }
        
        const cleanCategory = category.trim().toLowerCase();
        
        // Log to database for research purposes
        await logCategorySubmission(room.dbGameId, player.dbPlayerId, cleanCategory, false);
        
        // Add to room's category submissions for display
        room.categorySubmissions.push({
            playerId: mapping.playerId,
            nickname: player.nickname,
            category: cleanCategory
        });
        
        // Send confirmation to submitter
        socket.emit('category-submitted', { category: cleanCategory });
        
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('categories-update', {
                categorySubmissions: room.categorySubmissions
            });
        }

        console.log(`Player ${player.nickname} submitted category "${cleanCategory}"`);

    });

    // Host adds category via display interface
    socket.on('host-add-category', async (data) => {
        const { category } = data;
        const room = findRoomBySocketId(socket.id);
        
        if (!room || room.displaySocketId !== socket.id) {
            socket.emit('error', { message: 'Not authorized - display only' });
            return;
        }
        
        if (!category || category.trim().length === 0) {
            socket.emit('error', { message: 'Category cannot be empty' });
            return;
        }
        
        const cleanCategory = category.trim().toLowerCase();
        
        await logCategorySubmission(room.dbGameId, null, cleanCategory, false);
        
        room.categorySubmissions.push({
            playerId: 'host',
            nickname: 'Host',
            category: cleanCategory
        });
        
        socket.emit('category-added', { category: cleanCategory });
        socket.emit('categories-update', {
            categorySubmissions: room.categorySubmissions
        });
        
        console.log(`Host added category "${cleanCategory}" in room ${room.code}`);
    });

    // Start game from lobby phase
    socket.on('start-lobby-game', async (data) => {
        const room = findRoomBySocketId(socket.id);
        
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
            
            // Broadcast game start
            try {
                broadcastGameState(room);
            } catch (error) {
                console.error('Error broadcasting game state:', error);
            }
            
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
        
        // Send current state to display
        updateDisplay(room);

        console.log(`Display connected to room ${roomCode}`);
    });

    // Player submits exemplar
    socket.on('submit-exemplar', async (data) => {
        const mapping = socketToPlayer.get(socket.id);
        
        if (!mapping) {
            socket.emit('error', { message: 'Not in a room' });
            return;
        }
        const room = rooms.get(mapping.roomCode);
        const player = room.players.get(mapping.playerId);
        
        if (!room || !player) {
            socket.emit('error', { message: 'Player not found' });
            return;
        }

        if (room.gameState !== 'submitting') {
            socket.emit('error', { message: 'Not in submission phase' });
            return;
        }

        if (player.hasSubmitted) {
            socket.emit('error', { message: 'Already submitted' });
            return;
        }

        const { exemplar } = data;

        if (!exemplar || exemplar.trim().length === 0) {
            socket.emit('error', { message: 'Exemplar cannot be empty' });
            return;
        }
        try {
            const submissionDbId = await logSubmission(room.currentRoundDbId, player.dbPlayerId, exemplar.trim());

            room.submissions.push({
                playerId: mapping.playerId,  // Use playerId, not socketId
                nickname: player.nickname,
                exemplar: exemplar.trim(),
                votes: new Map(),
                dbSubmissionId: submissionDbId
            });

            player.hasSubmitted = true;
            socket.emit('submission-confirmed', { exemplar: exemplar.trim() });

            // Single broadcast handles all updates
            try {
                broadcastGameState(room);
            } catch (error) {
                console.error('Error broadcasting game state:', error);
            }
            
            // Check for early completion
            checkSubmissionComplete(room);
            
            console.log(`Player ${player.nickname} submitted "${exemplar}"`);
        } catch (error) {
            console.error('Error submitting exemplar:', error);
            socket.emit('error', { message: 'Failed to submit exemplar' });
        }
    });


    socket.on('submit-votes', async (data) => {
        const mapping = socketToPlayer.get(socket.id);
        
        if (!mapping) {
            socket.emit('error', { message: 'Not in a room' });
            return;
        }

        const room = rooms.get(mapping.roomCode);
        const player = room.players.get(mapping.playerId);
        
        if (!room || !player) {
            socket.emit('error', { message: 'Player not found' });
            return;
        }

        if (room.gameState !== 'voting') {
            socket.emit('error', { message: 'Not in voting phase' });
            return;
        }

        if (player.hasVoted) {
            socket.emit('error', { message: 'Already voted' });
            return;
        }

        const { votes } = data;

        try {
            // Record votes using playerId instead of socketId
            for (const [exemplarIndex, vote] of Object.entries(votes || {})) {
                const index = parseInt(exemplarIndex, 10);
                if (Number.isInteger(index) && room.submissions[index]) {
                    room.submissions[index].votes.set(mapping.playerId, !!vote);
                    await logVote(room.submissions[index].dbSubmissionId, player.dbPlayerId, !!vote);
                }
            }

            player.hasVoted = true;

            // Single broadcast handles all updates
            try {
                broadcastGameState(room);
            } catch (error) {
                console.error('Error broadcasting game state:', error);
            }
            
            // Check for early completion
            checkVotingComplete(room);
            
            console.log(`Player ${player.nickname} voted in room ${room.code}`);
        } catch (error) {
            console.error('Error submitting votes:', error);
            socket.emit('error', { message: 'Failed to submit votes' });
        }
    });


    // GM ends game
    socket.on('end-game', async () => {
        const room = findRoomBySocketId(socket.id);
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

        // Send to all connected players
        room.players.forEach(player => {
            if (player.isConnected && player.socketId) {
                io.to(player.socketId).emit('game-ended', { finalScores });
            }
        });
        
        if (room.displaySocketId) {
            io.to(room.displaySocketId).emit('show-round-scoreboard', {
                players: finalScores,
                round: room.round,
                isGameWide: true,
                isFinal: true
            });
        }

        console.log(`Game ended in room ${room.code}`);
    });

    // Handle disconnections
    socket.on('disconnect', async () => {
        console.log('Client disconnected:', socket.id);

        const mapping = socketToPlayer.get(socket.id);
        if (!mapping) return;

        const room = rooms.get(mapping.roomCode);
        if (!room) {
            removeSocketMapping(socket.id);
            return;
        }

        if (room.gmSocketId === socket.id) {
            // GM disconnected
            room.gmSocketId = null;
            room.players.forEach(player => {
                if (player.isConnected && player.socketId) {
                    io.to(player.socketId).emit('gm-disconnected');
                }
            });
            // Grace period before cleanup
            setTimeout(() => {
                if (rooms.has(mapping.roomCode) && !room.gmSocketId) {
                    rooms.delete(mapping.roomCode);
                    console.log(`Room ${mapping.roomCode} deleted after GM grace period`);
                }
            }, 300000); // 5 minutes
            
            console.log(`GM disconnected from room ${mapping.roomCode}`);

         } else if (room.displaySocketId === socket.id) {
            room.displaySocketId = null;
            console.log(`Display disconnected from room ${mapping.roomCode}`);

        } else {
            // Player disconnected
            const player = room.players.get(mapping.playerId);
            if (player) {
                player.isConnected = false;
                player.socketId = null;

                try {
                    await setPlayerConnected(mapping.playerId, false);
                } catch (error) {
                    console.error('Error updating player connection status:', error);
                }

                // Broadcast disconnection
                try {
                    broadcastGameState(room);
                } catch (error) {
                    console.error('Error broadcasting game state:', error);
                }

                console.log(`Player ${player.nickname} disconnected from room ${mapping.roomCode}`);

                // Extended grace period for players
                setTimeout(() => {
                    const currentPlayer = room.players.get(mapping.playerId);
                    if (currentPlayer && !currentPlayer.isConnected) {
                        // Could remove player here if desired
                        console.log(`Player ${player.nickname} still disconnected after grace period`);
                    }
                }, 600000); // 10 minutes
            }
        }
        removeSocketMapping(socket.id);
    });
});

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