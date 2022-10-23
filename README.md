# PositionGrapher
Short description:
This is a tool to provide insights and a visual way to analyze positions in the currency exchange market. Using Pandas, Plotly and other data science and visualization libraries, the data is processed, organized and presented in a graphical format.

# Example:
![alt text](https://github.com/NirOren10/PositionGrapher/blob/master/Position_plot.png?raw=true)

## The architecture:
![alt text](https://github.com/NirOren10/PositionGrapher/blob/master/architecture.png?raw=true)

## Long Description:
For a brief introduction to currency trading (Forex) refer to:
https://www.etoro.com/trading/a-guide-to-trading-currencies/#What-is-currency-trading

### How do our systems work?
The rate collector:
It collects market data from many data sources in real-time (see data section). 
The Signaler:
uses an advanced algorithm to detect when is a good time to enter a position based on changes in the market. When it thinks we should enter a position, it sends us an indication that we call a “signal”.
The Trader:
Based on the signals, sends requests to the brokers to exchange at a certain rate. The request contains:
If we want to buy or sell the currency
At what price we want to buy/sell (the rate changes hundreds of times per second, so it needs to specify)
How much we want to buy/sell

At the end of a day of trading, we need to analyze our positions to make improvements to our trading algorithm. The best way to analyze the data is to look at the market at the time of the position. 

#### Process Walkthrough
This Grapher process runs on our AWS EC2
It retrieves the Market Data from AWS S3 and Position Data from MongoDB. (See “The Data” section below)
The Market Data is then merged into a uniform DataFrame
Signals in the data are found and kept in memory
The positions are matched to the right signals, making sure the position is not an accident.
A graph is created, showing the market prices during the position, and infromation about the position in vertical lines and in the index. The graph also includes any signals that happened before or after the position.
The graph is then saved on AWS S3

## The Data:
This project relies on two essential components:

Market data (AWS S3): This is a long text file containing millions of quotes that data sources send our system to tell us the updated exchange rate for EUR/USD. An example of a quote:

Timestamp | Source | Type | Rate 
 --- | --- | --- |--- 
1621890000200 | Source A | offer | 1.22050 
	
- Timestamp: 
Indication of the time in Unix Timestamp. For Reference: https://www.unixtimestamp.com/
- Bank name: 
name of the source that sent us the quote
- Type: 
offer/bid. Offer is showing the price at which the bank is willing to sell to you that currency, while the bid is the price at which the bank is willing to buy the currency from you.
- Rate:
The exchange rate of Dollars to Euros, USD to EUR. 

Position Data(MongoDB): This is a list of the trading positions on a certain day. Each position includes:
SignalTimestamp:
Our complex algorithm processes data and sends us signals, which means that now is a very good time to enter a position. A signal can either recommend entering a Buy or a Sell position. (See bottom for definitions of Buy and Sell).
Direction:
Type of position, Buy or Sell. 
EnterRequestTimestamp:
The timestamp at which our systems sent the bank a request to buy/sell a currency to enter a position(depends on the type of position, buy/sell)
EnterRequestedPrice:
The price at which we want to buy/sell
EnterTime:
The timestamp in which the bank indicated the success of our enter request
EnterPrice:
The price at which the bank sells us/buys the currency from us. Notice this can be different from the requested price.
ExitRequestTimestamp:
The timestamp at which our systems sent the bank a request to buy/sell a currency to exit the position(depends on the type of position, buy/sell)
ExitRequestedPrice:
The timestamp at which our systems sent the bank a request to buy/sell a currency to exit the position(depends on the type of position, buy/sell)
ExitTime:
The timestamp in which the bank indicated the success of our exit request
ExitPrice:
The price at which the bank sells us/buys the currency from us. Notice this can be different from the requested price.



## “Buy” vs. “Sell” Positions:
“Buy”: This is the classic and the most intuitive type of position, buying a coin at a low price and selling at a higher price. 
Example: 
12:00: I have 10 Dollars. The rate of Dollars to Euros is 1/2. I convert my Dollars to Euros and now I have 20 Euros.
12:05:  The rate of dollars to Euros has now changed to 1/1. I convert my Euros to Dollars and now have 20 Dollars.
I started at 10 Dollars and now have 20.
“Sell”: This is when one sells a currency, then buys it at a cheaper price. 
12:00: I have 10 Euros, the rate of Dollars to Euros is 1/2. I convert my Euros to Dollars and now I have 5 Dollars.
12:05:  the rate of dollars to Euros has now changed to 1/4. I convert my Dollars to Euros and now have 20 Euros.
I started at 10 Euros and now have 20.


## Built With
- Python
- pandas
- pymongo
- datetime
- numpy
- plotly
- boto3
- AWS
- MongoDB
