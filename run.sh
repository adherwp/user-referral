# Check if the virtual environment directory exists
if [ ! -d "venv" ]
then
    echo "💸Create venv"
    python3 -m venv venv
    pip3 install --upgrade pip
fi
# Activate the virtual environment
echo "💸Activate venv"
source venv/bin/activate

# Upgrade pip within the virtual environment
echo "💸Activate venv"
pip3 install --upgrade pip

# Install the required packages
echo "💸Install pip packages"
pip3 install -r requirements.txt

# Run the Python script
# source .env
echo "💸 Running main.py"
python main.py
