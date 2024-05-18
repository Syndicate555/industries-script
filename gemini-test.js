const { GoogleGenerativeAI } = require('@google/generative-ai');
require('dotenv').config();
// Access your API key as an environment variable (see "Set up your API key" above)
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

async function run() {
	// For text-only input, use the gemini-pro model
	const model = genAI.getGenerativeModel({ model: 'gemini-pro' });

	const prompt = 'What are the 10 biggest companies in the S&P 500';

	const result = await model.generateContent(prompt);
	const response = await result.response;
	const text = response.text();
	console.log(response.text());
}

(async () => {
	try {
		await run();
	} catch (error) {
		console.error(error);
	}
	return process.exit();
})();
