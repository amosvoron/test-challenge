const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();

// Serve static assets from the "public" directory
app.use('/public', express.static(path.join(__dirname, 'public')));

// Set view engine to ejs
app.set('view engine', 'ejs');
app.use('/static', express.static('node_modules'));

const default_start_date = "2015-09-07"
const default_end_date = "2015-09-08"

// Read from local JSON file if "too many request" error (=429) occurs
function read_local_data(res, next) {
    try {
        fs.readFile(path.join(__dirname, '/data.json'), 'utf8', (err, data) => {
            if (err) {
                return next(new Error('Failed to read local data'));
            }
            const localData = JSON.parse(data);
            const asteroids = localData.near_earth_objects[default_start_date];
            res.render('index', { asteroids });
        });
    } catch (error) {
        return next(new Error(error.message));
    }
}

app.get('/', async (req, res, next) => {
    try {
        url = `https://api.nasa.gov/neo/rest/v1/feed?start_date=${default_start_date}&end_date=${default_end_date}&api_key=DEMO_KEY`
        const response = await axios.get(url);
        const asteroids = response.data.near_earth_objects[default_start_date];
        res.render('index', { asteroids });
    } catch (error) {
        if (error.response && error.response.status === 429) {
            read_local_data(res, next);
        } else {
            return next(new Error(error.message));
        }
    }
});

// Search route
app.get('/search', async (req, res, next) => {
    let startDate = req.query['start-date'];
    let endDate = req.query['end-date'];

    if (startDate === undefined || startDate == '') {
        startDate = default_start_date
    } 
    if (endDate === undefined || endDate == '') {
        endDate = default_end_date
    }     

    // Validate the dates
    if (startDate > endDate) {
        return next(new Error('The start date cannot be greater than the end date.'));
    }

    try {
        url = `https://api.nasa.gov/neo/rest/v1/feed?start_date=${startDate}&end_date=${endDate}&api_key=DEMO_KEY`
        const response = await axios.get(url);
        const asteroids = response.data.near_earth_objects[startDate];
        res.render('index', { asteroids });
    } catch (error) {
        if (error.response && error.response.status === 429) {
            read_local_data(res, next);
        } else {
            return next(new Error(error.message));
        }
    }
});

// This middleware catches any error thrown in route handlers
app.use((error, req, res, next) => {
    res.status(500).render('error', { errorMessage: error.message });
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server started on http://localhost:${PORT}`);
});


