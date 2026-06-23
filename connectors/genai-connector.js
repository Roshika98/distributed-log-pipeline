const genai = require("@google/genai");

class GenAIConnector {
	static instance = null;

	static getConnector() {
		if (!GenAIConnector.instance) {
			const genaiConnector = new GenAIConnector();
			GenAIConnector.instance = genaiConnector;
		}
		return GenAIConnector.instance;
	}

	constructor() {
		this.client = new genai.GoogleGenAI({
			enterprise: false,
			apiKey: process.env.GEMINI_API_KEY,
		});
	}
}
