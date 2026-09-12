const express = require('express');

const app = express();

const capacity = 5;
const refillRate = 1 // 1 per sec;

const clients = new Map();
function tokenBucket(req,res,next){
    
      const ip = req.ip;
      const now = Date.now();
      let client = clients.get(ip);

     if(!client){
        client = {
          tokens: capacity,
          lastReFill: now
        }
       clients.set(ip,client);
     }      

  const time = (now - client.lastReFill) / 1000;

  const newTokens = time * refillRate;

  client.tokens = Math.min(capacity,client.tokens+newTokens);
  client.lastReFill = now;
  if(client.tokens<1){
     return res.status(429).json({
       error:"too many request"
     });
  }

  client.tokens--;
  next();
}
app.use(tokenBucket);

app.get("/", (req, res) => {
    res.json({
        message: "Success"
    });
});

app.listen(3000, () => {
    console.log("Server running on 3000");
});
