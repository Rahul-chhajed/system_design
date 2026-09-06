/*
  api gateway

                           Internet
                            │
                            ↓
                     ┌─────────────┐
                     │ API Gateway │
                     │    :3000    │
                     └──────┬──────┘
                            │
             ┌──────────────┼───────────────┐
             │              │               │
             ↓              ↓               ↓
        User Service   Product Service   Order Service
             │              │               │
             │              ↓               │
             │        ┌─────────────┐       │
             │        │Load Balancer│       │
             │        └──────┬──────┘       │
             │          ┌────┼────┐         │
             │          ↓    ↓    ↓         │
             │         P1   P2   P3         │

*/


const express = require('express');
const {createProxyMiddleware} = require('http-proxy-middleware');

const app = express();
function authenticate(req,res,next){

    const token = req.headers.authorization;

    if(!token){
        return res.status(401).json({
            error:'Authentication required'
        });
    }

    // jwt verify here

    next();
}


app.use('/user', authenticate,createProxyMiddleware({
    target:'http://localhost:3001',
    changeOrigin:true
}));

app.use('/product',authenticate, createProxyMiddleware({
    target:'http://localhost:3001',
    changeOrigin:true
}));

app.use('/order',authenticate,createProxyMiddleware({
    target:'http://localhost:3001',
    changeOrigin:true
}));


app.listen(3000,()=>{
    console.log('listening on port 3000');
});


