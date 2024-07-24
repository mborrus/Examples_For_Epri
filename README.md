# Model-Validation-GCP

The order of processes is:

1. Daily_Runner.py
2. PKL_Generation_Runner.py
3. PKL_Testing_Runner.py
4. Forecast_Comparison_Runner.py

1. Daily Runner collects IBM and Sensor data daily
2. PKL Generation creates the models daily after the sensor and ibm data is created
3. PKL Testing produces forecasts from these new models
4. Forecast Comparison produces error statistics from these new models
