const express = require('express');
const {createProxyMiddleware} = require('http-proxy-middleware');


const app=express();

const servers = [
    'http://localhost:3001',
    'http://localhost:3002',
    'http://localhost:3003'
];

let currentServer=0;

app.use((req,res,next)=>{
    // round robin server allocation  
    const target= servers[currentServer];

    currentServer = (currentServer+1)%servers.length;
        
    createProxyMiddleware({
        target:target,
        changeOrigin:true
    })(req,res,next);

    /*
     this above code is same as
     const proxy=createProxyMiddleware({
                target:target,
                cahngeOrigin:true
     });

     proxy(req,res,next);

     */

    
})

app.listen(3000,()=>{
    console.log('running on port 3000');
});
