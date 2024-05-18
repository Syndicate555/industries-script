const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { createObjectCsvWriter } = require('csv-writer');
const OpenAI = require('openai');
const stream = require('stream');
const util = require('util');

// Configuration
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const patentsFilePath = path.join(__dirname, 'patents.csv');
const industriesFilePath = path.join(__dirname, 'unique_industries.txt');
const outputFilePath = path.join(__dirname, 'patents_with_industries.csv');
const errorFilePath = path.join(__dirname, 'patents_errors.csv');

const openai = new OpenAI({
	apiKey: OPENAI_API_KEY,
});

const finished = util.promisify(stream.finished);

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

// Function to call OpenAI API
async function getIndustryForOrganization(organization, industries) {
	const prompt = `
    Given the following list of industries:
    ${industries.join(', ')}

    Determine the industry that best matches the following organization:
    ${organization}

    Return only the industry name.
  `;

	try {
		const response = await openai.chat.completions.create({
			model: 'gpt-3.5-turbo',
			messages: [
				{
					role: 'user',
					content: prompt,
				},
			],
			stream: true,
		});
		return response.data.choices[0].text.trim();
	} catch (error) {
		console.error('Error calling OpenAI API:', error);
		return null;
	}
}

// Process patents CSV file and add industry column
const processPatentsFile = async () => {
	const errors = [];
	const records = [];

	const outputWriteStream = fs.createWriteStream(outputFilePath);
	const errorWriteStream = fs.createWriteStream(errorFilePath);

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

	readStream.on('data', (row) => {
		records.push(row);
	});

	readStream.on('end', async () => {
		console.log(`Read ${records.length} records from patents file`);

		const batchSize = 100;
		for (let i = 0; i < records.length; i += batchSize) {
			const batch = records.slice(i, i + batchSize);

			const promises = batch.map(async (row) => {
				const organization = row.organization;
				const industry = await getIndustryForOrganization(
					organization,
					industries
				);
				if (industry) {
					row.industry = industry;
				} else {
					errors.push(row);
				}
				return row;
			});

			const processedBatch = await Promise.all(promises);
			await csvWriter.writeRecords(processedBatch);
			console.log(`Processed batch ${i / batchSize + 1}`);

			// Respect API rate limits
			await new Promise((resolve) => setTimeout(resolve, 1000));
		}

		if (errors.length > 0) {
			await errorCsvWriter.writeRecords(errors);
			console.log(`Logged ${errors.length} errors to ${errorFilePath}`);
		}

		console.log('CSV file successfully processed');
	});

	readStream.on('error', (error) => {
		console.error('Error processing patents file:', error);
	});

	await finished(readStream);
};

// Run the process
processPatentsFile();
