const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');
const OpenAI = require('openai');
const stream = require('stream');
const util = require('util');
const Bottleneck = require('bottleneck');

const finished = util.promisify(stream.finished);

// Configuration
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const patentsFilePath = path.join(__dirname, 'patents.csv');
const industriesFilePath = path.join(__dirname, 'unique_industries.txt');
const outputFilePath = path.join(__dirname, 'patents_with_industries.csv');
const errorFilePath = path.join(__dirname, 'patents_errors.csv');

const openai = new OpenAI({
	apiKey: OPENAI_API_KEY,
});

// Read industries from file
let industries = [];
fs.readFile(industriesFilePath, 'utf8', (err, data) => {
	if (err) {
		console.error('Error reading industries file:', err);
		process.exit(1);
	}
	industries = data
		.split('\n')
		.map((line) => line.trim())
		.filter((line) => line.length > 0);
});

const limiter = new Bottleneck({
	maxConcurrent: 1,
	minTime: 120, // 120 ms delay between requests
});

// Function to call OpenAI API
async function getIndustryForOrganization(organization, industries) {
	// console.log(industries.join(', '));
	const prompt = `
    Given the following list of industries:
    ${industries.join(', ')}

    Determine the industry that best matches the following organization:
    ${organization}

    Return only the industry name exactly how it appears in the list. Do not return any other information. Return unknown if the industry is not in the list. 
  `;

	try {
		const response = await limiter.schedule(() =>
			openai.chat.completions.create({
				model: 'gpt-3.5-turbo',
				messages: [
					{
						role: 'user',
						content: prompt,
					},
				],
			})
		);
		console.log(response.choices[0].message.content.trim().toLowerCase());
		return response.choices[0].message.content.trim().toLowerCase();
	} catch (error) {
		console.error('Error calling OpenAI API:', error);
		return null;
	}
}

// Process patents CSV file and add industry column
const processPatentsFile = async () => {
	const errors = [];

	const csvWriter = createObjectCsvWriter({
		path: outputFilePath,
		header: [
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
	});

	const errorCsvWriter = createObjectCsvWriter({
		path: errorFilePath,
		header: [
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
		],
	});

	const readStream = fs.createReadStream(patentsFilePath).pipe(csv());

	readStream.on('data', async (row) => {
		readStream.pause();
		const organization = row.organization;
		const industry = await getIndustryForOrganization(organization, industries);
		if (industry) {
			row.industry = industry;
			await csvWriter.writeRecords([row]);
		} else {
			errors.push(row);
			await errorCsvWriter.writeRecords([row]);
		}
		readStream.resume();
	});

	readStream.on('end', () => {
		console.log('CSV file successfully processed');
		if (errors.length > 0) {
			console.log(`Logged ${errors.length} errors to ${errorFilePath}`);
		}
	});

	readStream.on('error', (error) => {
		console.error('Error processing patents file:', error);
	});

	await finished(readStream);
};

// Run the process
processPatentsFile();
