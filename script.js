const fs = require('fs');
const csv = require('csv-parser');
const csvWriter = require('csv-write-stream');
const { Worker } = require('worker_threads');
const { pipeline } = require('stream');
const { Transform } = require('stream');

function createTransformStream(companyLookup, writerMatched, outputUnmatched) {
	let buffer = [];
	const flushBuffer = async () => {
		const results = await Promise.all(
			buffer.map(
				(row) =>
					new Promise((resolve) => {
						const worker = new Worker('./fuzzyMatcher.js', {
							workerData: { row, companyLookup },
						});
						worker.on('message', (result) => resolve(result));
					})
			)
		);

		results.forEach((result) => {
			if (result.industry !== 'Unknown') {
				writerMatched.write(result);
			} else {
				outputUnmatched.write(`${result.organization}\n`);
			}
		});

		buffer = [];
	};

	return new Transform({
		objectMode: true,
		transform(row, encoding, callback) {
			buffer.push(row);
			if (buffer.length >= 500) {
				// Batch size of 500, adjust as needed
				flushBuffer().then(() => callback());
			} else {
				callback();
			}
		},
		flush(callback) {
			if (buffer.length > 0) {
				flushBuffer().then(() => callback());
			} else {
				callback();
			}
		},
	});
}

function processFiles() {
	const companyLookup = {};
	const outputMatched = fs.createWriteStream('matched_patents.csv');
	const outputUnmatched = fs.createWriteStream('unmatched_companies.txt');

	const writerMatched = csvWriter({
		headers: [
			'patent_id',
			'location_id',
			'country',
			'state',
			'city',
			'june2022assignee_id',
			'assignee_sequence',
			'assignee_type',
			'organization',
			'university',
			'Found in OC',
			'jurisdiction_code',
			'company_number',
			'UO_DISCERN',
			'SUB_DISCERN',
			'gvkey',
			'CUSIP',
			'founding_year',
			'first_year_publicly_listed',
			'founding_score',
			'VC_backed_assignee',
			'VC_score',
			'industry',
		],
		sendHeaders: true,
	});

	fs.createReadStream('test-companies.csv')
		.pipe(csv())
		.on('data', (row) => {
			const normalized = row.name
				.toLowerCase()
				.replace(/[\.,-\/#!$%\^&\*;:{}=\-_`~()]/g, '')
				.replace(/\s{2,}/g, ' ');
			companyLookup[normalized] = row.industry;
		})
		.on('end', () => {
			const transformStream = createTransformStream(
				companyLookup,
				writerMatched,
				outputUnmatched
			);
			pipeline(
				fs.createReadStream('test.csv'),
				csv(),
				transformStream,
				(err) => {
					if (err) {
						console.error('Pipeline failed.', err);
					} else {
						writerMatched.pipe(outputMatched);
						console.log('Pipeline completed successfully.');
					}
				}
			);
		});
}

processFiles();
