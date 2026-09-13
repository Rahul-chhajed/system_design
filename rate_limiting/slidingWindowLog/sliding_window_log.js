const express = require('express');

const app = express();

const LIMIT=5;
const WINDOW = 10 * 1000;

const clients = new Map();
function slidingWindowLog(req,res,next){
  const ip = req.ip;
  const now = Date.now();

  let timestamps = clients.get(ip);

  if(!timestamps){
    timestamps = [];
    clients.set(ip,timestamps);
  }

  while(timestamps.length>0 && timestamps[0]<=now-WINDOW){
    timestamps.shift();
  }

  if(timestamps.length>=LIMIT){
    return res.status(429).json({
      error: "too many request"
    });
  }

  timestamps.push(now);

  next();
}

app.use(slidingWindowLog);

app.listen(3000,()=>{
  console.log('server listening on port 3000');
});


