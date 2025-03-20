# Use official Python image as base
FROM python:3.10

# Set the working directory inside the container
WORKDIR /app

# Copy all files to the container
COPY . /app/

# Update package list and install FFMPEG
RUN apt-get update && apt-get install -y ffmpeg

# Create a symbolic link to ensure ffmpeg is accessible
RUN ln -s /usr/bin/ffmpeg /usr/local/bin/ffmpeg

# Verify FFMPEG installation
RUN which ffmpeg && ffmpeg -version

# Ensure ffmpeg is accessible in PATH
ENV PATH="/usr/bin:${PATH}"

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the required port
EXPOSE 8080

# Run the bot
CMD ["python", "bot.py"]
