'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]), encoding: 'latin1'})

console.log("Start Server");

function parseValue(value) {
	// Try to parse the value as a number
	const numericValue = parseFloat(value);

	// Check if parsing was successful, and return either the number or the original value
	return isNaN(numericValue) ? value : numericValue;
}

function rowToMap(row) {
	const stats = {};

	row.forEach(function (item) {
		const { column, $ } = item;
		const columnName = column.replace('cf:', '');

		// Check if the current column is 'season' and modify its format
		if (columnName === 'season') {
			const seasonYear = parseInt($, 10);
			stats[columnName] = `${seasonYear}-${(seasonYear + 1).toString().substr(2)}`;
		} else {
			stats[columnName] = parseValue($);
		}
	});

	return stats;
}


app.use(express.static('public'));
app.get('/delays.html', function (req, res) {
	const player = req.query['origin'];
	console.log(player);

	// Get data from the Hbase table
	hclient.table('hsiangct_final_NBA_stats').scan({
		filter: {
			type: "PrefixFilter",
			value: player
		},
	}, function (err, cells) {
		if (err) {
			console.error(err);
			return res.status(500).send('Error processing data');
		}

		// Group the data in the format of mustache
		let groupedData = [];
		let currentGroup = [];
		cells.forEach(function (cell, index) {
			currentGroup.push(rowToMap([cell]));

			if ((index + 1) % 21 === 0 || index === cells.length - 1) {
				groupedData.push(Object.assign({}, ...currentGroup));
				currentGroup = [];
			}
		});

		filesystem.readFile("result.mustache", "utf8", (err, template) => {
			if (err) {
				console.error(err);
				return res.status(500).send('Error loading template');
			}

			let html = mustache.render(template, {
				season_data: groupedData,
				player_name: player // Include the player's name here
			});
			res.send(html);
		});
	});
});

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

// Middleware to parse JSON and form data
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve static files (CSS, images, etc.)
app.use(express.static('public'));

app.post('/submit-stat.html', function(req, res) {
	const { player_name, season, age, points, rebounds, assists, player_height,
		player_weight, team_abbreviation, college, country, draft_year, draft_round, draft_number, gp, net_rating, oreb_pct, dreb_pct, usg_pct, ts_pct, ast_pct } = req.body;

	var report = {
		key : player_name + season,
		player_name : player_name,
		season : season,
		age : age,
		pts : points,
		reb : rebounds,
		ast : assists,
		player_height : player_height,
		player_weight : player_weight,
		team_abbreviation : team_abbreviation,
		college : college,
		country : country,
		draft_year: draft_year,
		draft_round : draft_round,
		draft_number : draft_number,
		gp : gp,
		net_rating : net_rating,
		oreb_pct : oreb_pct,
		dreb_pct : dreb_pct,
		usg_pct : usg_pct,
		ts_pct : ts_pct,
		ast_pct : ast_pct
	}

	const payloads = [
		{ topic: 'hsiangct_final_NBA', messages: JSON.stringify(report) }
	];

	kafkaProducer.send(payloads, function(err, data) {
		if (err) {
			console.error('Error sending message to Kafka:', err);
			return res.status(500).send('Error sending message');
		}
		console.log('stat sent')
		res.redirect('submit-stat.html');
	});
});

/*
app.post('/trigger-batch-update', function(req, res) {
	const upToTimestamp = new Date().toISOString();

	const batchUpdateMessage = {
		type: 'batch_update',
		upToTimestamp: upToTimestamp
	};

	const payloads = [
		{ topic: 'hsiangct_batch_update', messages: JSON.stringify(batchUpdateMessage) }
	];

	kafkaProducer.send(payloads, function(err, data) {
		if (err) {
			console.error('Error sending batch update message to Kafka:', err);
			return res.status(500).send('Error sending batch update message');
		}
		console.log('Batch update message sent');
		res.status(200).send('Batch update triggered');
	});
});*/



app.listen(port);
