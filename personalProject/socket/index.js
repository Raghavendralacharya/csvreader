const express = require('express');
const app = express();
const http = require('http');
const server = http.createServer(app);
const { Server } =  require("socket.io")
const io = new Server(server);
const { v4: uuidv4 } = require("uuid");

app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});
let count = 0
let user = {}
io.on('connection',(socket)=>{
    let userId = uuidv4()
    let userName = 'user' + count;
    count++;
    console.log("a user connected.", userId, socket.id)
    user[socket.id] = userName
    // io.emit('chat message', "a user connected.");
    // socket.broadcast.emit('chat message',"user-connected ", userName);
    socket.on('disconnect', () => {
        console.log('user disconnected');
        // socket.broadcast.emit('chat message',"user disconnected " + "userId");
    });
    socket.on('chat message', (msg, id) => {
        console.log('message: ' + msg, id);
        io.emit('chat message', msg, user[id],id);
    }); 
    
    socket.on("join-room", (roomId, userId, userName) => {
        socket.join(roomId);
        socket.to(roomId).broadcast.emit("user-connected", userId);
        socket.on("message", (message) => {
          io.to(roomId).emit("createMessage", message, userName);
        });
      });
})
// io.emit('some event', { someProperty: 'some value', otherProperty: 'other value' }); // This will emit the event to all connected sockets

server.listen(3002, () => {
  console.log('listening on *:3000');
});