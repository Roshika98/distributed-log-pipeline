const genai = require("@google/genai");

class GenAIManager {
	static instance;

	static async ask() {
		if (!this.instance) {
			throw new Error("GenAIManager is not initialized");
		}

		const response = await this.instance.client.models.generateContent({
			model: "gemini-2.5-flash",
			contents: "Hello, how are you?",
		});
		return response;
	}

	static Init() {
		if (!this.instance) {
			this.instance = new GenAIManager();
		} else {
			console.warn("GenAIManager is already initialized");
		}
	}

	constructor() {
		this.client = new genai.GoogleGenAI({
			enterprise: false,
			apiKey: process.env.GEMINI_API_KEY,
		});
	}
}

module.exports = GenAIManager;
