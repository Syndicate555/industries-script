const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const inputFilePath = path.join(__dirname, 'companies.csv'); // Adjust the path to your CSV file
const outputFilePath = path.join(__dirname, 'unique_industries.txt'); // Path to the output file

const uniqueIndustries = new Set();

fs.createReadStream(inputFilePath)
	.pipe(csv())
	.on('data', (row) => {
		if (row.industry) {
			uniqueIndustries.add(row.industry.trim());
		}
	})
	.on('end', () => {
		const uniqueIndustriesArray = Array.from(uniqueIndustries);
		fs.writeFile(outputFilePath, uniqueIndustriesArray.join('\n'), (err) => {
			if (err) {
				console.error('Error writing to the file:', err);
			} else {
				console.log(
					'Unique industries have been written to unique_industries.txt'
				);
			}
		});
	})
	.on('error', (error) => {
		console.error('Error reading the CSV file:', error);
	});
