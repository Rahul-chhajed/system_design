//long polling

const express = require('express');

const app = express();

app.get('/data', (req, res) => {

    console.log('Client connected');

    setTimeout(() => {

        res.json({
            message: 'New data available'
        });

    }, 5000);

});

app.listen(3000, () => {
    console.log('Listening on port 3000');
});

