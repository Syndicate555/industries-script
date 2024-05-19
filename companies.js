const fs = require('fs');
const csvWriter = require('csv-write-stream');
const OpenAI = require('openai');
const Bottleneck = require('bottleneck');
const path = require('path');
require('dotenv').config();
const { finished } = require('stream/promises');
const csvParser = require('csv-parser');
const Anthropic = require('@anthropic-ai/sdk');

// const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_API_KEY = process.env.NEW_OPENAI_API_KEY;
const companiesFilePath = path.join(__dirname, 'unique_companies.txt');
const industriesFilePath = path.join(__dirname, 'unique_industries.txt');
const outputFilePath = path.join(__dirname, 'companies_with_industries.csv');
const errorFilePath = path.join(__dirname, 'companies_errors.txt');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

const anthropic = new Anthropic({
	apiKey: process.env.CLAUDE_API_KEY,
});

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

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

// Bottleneck to manage API rate limiting
const limiter = new Bottleneck({
	maxConcurrent: 1,
	minTime: 120,
});

// Function to call OpenAI API
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
				max_tokens: 4096,
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

// Function to read existing companies from the CSV file
async function readExistingCompanies(filePath) {
	return new Promise((resolve, reject) => {
		const companies = new Set();
		fs.createReadStream(filePath)
			.pipe(csvParser())
			.on('data', (row) => {
				companies.add(row.company);
			})
			.on('end', () => {
				resolve(companies);
			})
			.on('error', (error) => {
				reject(error);
			});
	});
}

// Process companies and assign industries
async function processCompaniesFile() {
	const companies = fs
		.readFileSync(companiesFilePath, 'utf8')
		.split('\n')
		.map((line) => line.trim())
		.filter((line) => line.length > 0);

	const existingCompanies = await readExistingCompanies(outputFilePath);
	const newCompanies = companies.filter(
		(company) => !existingCompanies.has(company)
	);
	console.log(newCompanies.length);
	const errors = [];

	const output = fs.createWriteStream(outputFilePath, { flags: 'a' });
	const csvWriterInstance = csvWriter({
		headers:
			!fs.existsSync(outputFilePath) || fs.statSync(outputFilePath).size === 0
				? ['company', 'industry']
				: false,
		sendHeaders:
			!fs.existsSync(outputFilePath) || fs.statSync(outputFilePath).size === 0,
	});

	csvWriterInstance.pipe(output);

	for (const company of newCompanies) {
		const industry = await getIndustryForCompanyGPT(company, industries);
		if (industry && industry.toLowerCase() !== 'unknown') {
			csvWriterInstance.write({ company, industry });
		} else {
			errors.push(company);
		}
	}

	csvWriterInstance.end();
	await finished(output);

	if (errors.length > 0) {
		fs.appendFileSync(errorFilePath, errors.join('\n') + '\n', 'utf8');
		console.log(`Logged ${errors.length} errors to ${errorFilePath}`);
	}

	console.log('Companies successfully processed');
}

// Run the process
processCompaniesFile();
