# FROM node:20-alpine 
FROM node:23-alpine

# Create a user and a group
# RUN addgroup app && adduser -S -G app app
# RUN mkdir /app && chown app:app /app
# USER app

# Create app directory
WORKDIR /app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./

# If you are building your code for production
# RUN npm ci --only=production
RUN npm install

# Bundle app source
COPY . .

EXPOSE 6321
CMD [ "node", "index.js" ]


