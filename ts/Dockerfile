# Use the official Node.js runtime as a parent image
FROM node:14

# Set the working directory in the container to /usr/app/src
WORKDIR /usr/app/src

# Copy package.json and package-lock.json to the root directory of the /usr/app/src
COPY package*.json ./

# Install the required node modules
RUN npm install

# Bundle the app source inside the Docker image
COPY . .

# Make port 3000 available outside the container
EXPOSE 3000

# Define the command to run the app using CMD
CMD ["node", "app.js"]
