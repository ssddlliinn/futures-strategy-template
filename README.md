1. price data outputs values of both day and night 
open/close/volume for TX values. The most important thing is 
that it comes with datetime object for date index to prevent mismatch.

2. modify get other data file to get other necessary data for indicators 
in strategy. return dataframe with date in datetime object and as index.

# 3. strategy class: 
	a. calculate the indicator
	b. get parameters
	c. buy/sell condition
	d. offset buy/sell condition

# 4. optimize
	a. set optimize parameters numbers and range
	b. set the decode and put values into para dict
	c. give all parameters name and its decode formula

# 5. vizualization:
  a. give the desired values to xi list and assign it to para dict with parameter names
  b. rename other data files to match the data inside.
