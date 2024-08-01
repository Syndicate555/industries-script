const fs = require('fs');
const csvWriter = require('csv-write-stream');
const OpenAI = require('openai');
const Bottleneck = require('bottleneck');
const path = require('path');
require('dotenv').config();
const { finished } = require('stream/promises');
const csvParser = require('csv-parser');
const Anthropic = require('@anthropic-ai/sdk');

const OPENAI_API_KEY = process.env.NEW_OPENAI_API_KEY;
const allCompaniesFilePath = path.join(__dirname, 'unique_companies.txt');
const allIndustriesFilePath = path.join(__dirname, 'unique_industries.txt');
const outputFilePathForExistingCompanies = path.join(
	__dirname,
	'companies_with_industries.csv'
);
const errorFilePath = path.join(__dirname, 'companies_errors.txt');

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const anthropic = new Anthropic({ apiKey: process.env.NEW_CLAUDE_API_KEY });

async function readIndustries(filePath) {
	return new Promise((resolve, reject) => {
		fs.readFile(filePath, 'utf8', (err, data) => {
			if (err) {
				reject(err);
			} else {
				const industries = data
					.split('\n')
					.map((line) => line.trim())
					.filter((line) => line.length > 0);
				resolve(industries);
			}
		});
	});
}

const limiter = new Bottleneck({
	maxConcurrent: 1,
	minTime: 1300,
});

async function getIndustryForCompanyGPT(company, industries) {
	const prompt = `
        Given the following list of industries:
        ${industries.join(', ')}
    
        Determine the industry that best matches the following organization:
        ${company}
    
        Return only the industry name exactly how it appears in the list. Do not return any other information. Return "unknown" if the industry is not in the list.
    `;
	try {
		const response = await limiter.schedule(() =>
			openai.chat.completions.create({
				model: 'gpt-3.5-turbo',
				messages: [{ role: 'user', content: prompt }],
			})
		);
		const industry = response.choices[0].message.content.trim().toLowerCase();
		console.log(`${company}:${industry}`);
		return industry;
	} catch (error) {
		console.error('Error calling OpenAI API:', error);
		return null;
	}
}

async function getIndustryForCompanyCLAUDE(company, industries) {
	const prompt = `
        Given the following list of industries:
        ${industries.join(', ')}
    
        Determine the industry that best matches the following organization:
        ${company}
    
        Return only the industry name exactly how it appears in the list. Do not return any other information. Return "unknown" if the industry is not in the list.
    `;
	try {
		const response = await limiter.schedule(() =>
			anthropic.messages.create({
				model: 'claude-3-haiku-20240307',
				max_tokens: 1024,
				messages: [
					{
						role: 'user',
						content: prompt,
					},
				],
			})
		);
		const industry = response.content[0].text.trim().toLowerCase();
		console.log(`${company}:${industry}`);
		return industry;
	} catch (error) {
		console.error('Error calling CLAUDE API:', error);
		return null;
	}
}

async function readExistingCompanies(filePath) {
	return new Promise((resolve, reject) => {
		const companies = new Set();

		const handleError = (error) => {
			console.error(`Error while reading file ${filePath}:`, error);
			reject(error);
		};

		const stream = fs.createReadStream(filePath);
		stream
			.on('error', handleError)
			.pipe(csvParser())
			.on('data', (row) => {
				if (row.company) {
					companies.add(normalizeCompanyName(row.company));
				}
			})
			.on('end', () => {
				console.log('Finished reading and parsing CSV file.');
				resolve(companies);
			})
			.on('error', handleError);
	});
}

function normalizeCompanyName(companyName) {
	// Normalize the company name to handle special characters, accents, etc.
	return companyName.toLowerCase().trim();
}

async function processCompaniesFile() {
	const industries = await readIndustries(allIndustriesFilePath);

	const allCompanies = fs
		.readFileSync(allCompaniesFilePath, 'utf8')
		.split('\n')
		.map((line) => line.toLowerCase().trim())
		.filter((line) => line.length > 0);

	const existingCompanies = await readExistingCompanies(
		outputFilePathForExistingCompanies
	);
	console.log(existingCompanies.has('Breezer Holdings, LLC'));
	// fs.writeFile('test2.txt', Array.from(existingCompanies).join('\n'), (err) => {
	// 	if (err) {
	// 		console.error(err);
	// 		return;
	// 	}
	// });
	const newCompanies = allCompanies.filter(
		(company) => !existingCompanies.has(company)
	);
	console.log(`New companies to process: ${newCompanies.length}`);
	const errors = [];

	const fileExists = fs.existsSync(outputFilePathForExistingCompanies);
	const isEmptyFile = fileExists
		? fs.statSync(outputFilePathForExistingCompanies).size === 0
		: true;

	const output = fs.createWriteStream(outputFilePathForExistingCompanies, {
		flags: 'a',
	});
	const csvWriterInstance = csvWriter({
		headers: isEmptyFile ? ['company', 'industry'] : false,
		sendHeaders: isEmptyFile,
	});

	csvWriterInstance.pipe(output);

	// for (const company of newCompanies) {
	// 	const industry = await getIndustryForCompanyGPT(company, industries);
	// 	csvWriterInstance.write({ company, industry });
	// }

	await new Promise((resolve) => csvWriterInstance.end(resolve));
	await finished(output);

	if (errors.length > 0) {
		fs.appendFileSync(errorFilePath, errors.join('\n') + '\n', 'utf8');
		console.log(`Logged ${errors.length} errors to ${errorFilePath}`);
	}

	console.log('Companies successfully processed');
}

processCompaniesFile();
