create schema zillow_db

/*Loader Function*/
drop table if exists zillow;
DROP LOADER IF EXISTS my_loader;
CREATE LOADER my_loader(filename STRING) LANGUAGE PYTHON {
	import pandas as pd
	
	zillow = pd.read_csv("../Large Scale Data Management/" + filename).to_dict('records')

	for row in range(len(zillow)):
    	_emit.emit(zillow[row]) 
};
create TABLE zillow FROM LOADER my_loader('zillow.csv');
SELECT * FROM zillow;


/*1. Extract number of bedrooms. 
 * You will implement a UDF that processes the facts_and_features column and extracts the number of bedrooms.*/
drop function zillow_db.bedrooms;
create function zillow_db.bedrooms (col STRING) RETURNS INTEGER
language PYTHON {
	return [int(x.split(" ")[0]) if x.split(" ")[0].isdigit() else None for x in col] 
};
SELECT zillow.facts_and_features, zillow_db.bedrooms(zillow.facts_and_features) AS bedrooms 
FROM zillow;
alter table zillow add bedrooms integer;
UPDATE zillow SET bedrooms = zillow_db.bedrooms(facts_and_features) WHERE 1=1;

/*2. Extract number of bathrooms. 
 * You will implement a UDF that processes the facts_and_features column and extracts the number of bathrooms.*/
drop function zillow_db.bathrooms;
CREATE FUNCTION zillow_db.bathrooms (col STRING) RETURNS INTEGER
LANGUAGE PYTHON {
	return [float(x.replace(" ", "").split(",")[1].replace("ba", "").replace(".0", "")) if x.replace(" ", "").split(",")[1].replace("ba", "").replace(".0", "").isdigit() else None for x in col] 
};
SELECT zillow.facts_and_features, zillow_db.bathrooms(zillow.facts_and_features) AS bathrooms 
FROM zillow LIMIT 10;
alter table zillow add bathrooms integer;
UPDATE zillow SET bathrooms = zillow_db.bathrooms(facts_and_features) WHERE 1=1;

/*3. Extract sqft. 
 * You will implement a UDF that processes the facts_and_features column and extracts the sqft.*/
drop function zillow_db.sqft;
CREATE FUNCTION zillow_db.sqft (col STRING) RETURNS INTEGER
LANGUAGE PYTHON {
	return [int(x.split(",")[2].split(" ")[0]) if x.split(",")[2].split(" ")[0].isdigit() else None for x in col] 
};
SELECT zillow.facts_and_features, zillow_db.sqft(zillow.facts_and_features) AS sqft 
FROM zillow LIMIT 10;
alter table zillow add sqft integer;
UPDATE zillow SET sqft = zillow_db.sqft(facts_and_features) WHERE 1=1;

/*4. Extract type. 
 * You will implement a UDF that processes the title column and returns the type of the listing (e.g. condo, house, apartment)*/
drop function zillow_db.house_type;
CREATE FUNCTION zillow_db.house_type (col STRING) RETURNS STRING
LANGUAGE PYTHON {
	return [x.split(" ")[0] if x.split(" ")[0].isalpha() else None for x in col] 
};
SELECT zillow.title, zillow_db.house_type(zillow.title) AS house_type 
FROM zillow LIMIT 10;
alter table zillow add house_type string;
UPDATE zillow SET house_type = zillow_db.house_type(title) WHERE 1=1;

/*5. Extract offer. 
 * You will implement a UDF that processes the title column and returns the type of offer. 
 * This can be sale, rent, sold, forclose*/
drop function zillow_db.offer;
CREATE FUNCTION zillow_db.offer (col STRING) RETURNS STRING
LANGUAGE PYTHON {
	types = ['sale', 'rent', 'sold', 'foreclosure']
	return [x.split(" ")[-1] if x.split(" ")[-1].lower() in types else None for x in col]
};
SELECT zillow.title, zillow_db.offer(zillow.title) AS offer 
FROM zillow LIMIT 10;
alter table zillow add offer string;
UPDATE zillow SET offer = zillow_db.offer(title) WHERE 1=1;


/*6. Filter out listings that are not for sale.*/
SELECT * FROM zillow WHERE offer <> 'sale' limit 5;

/*7. Extract price. 
 * You will implement a UDF that processes the price column and extract the price. 
 * Prices are stored as strings in the CSV. 
 * This UDF parses the string and returns the price as an integer.*/
drop function zillow_db.newprice;
CREATE FUNCTION zillow_db.newprice (col STRING) RETURNS FLOAT
LANGUAGE PYTHON {
	import re
	return [float(re.sub("[^0-9]", "", x)) for x in col]
};

SELECT zillow.price, zillow_db.newprice(zillow.price) AS newprice 
FROM zillow LIMIT 10;
alter table zillow add newprice float;
UPDATE zillow SET newprice = zillow_db.newprice(price) where 1=1;

/*8. Filter out listings with more than 10 bedrooms*/
SELECT * FROM zillow WHERE bedrooms > 10 limit 5;

/*9. Filter out listings with price greater than 20000000 and lower than 100000*/
SELECT * FROM zillow WHERE (newprice > 20000000 or newprice < 100000)  limit 5;

/*10. Filter out listings that are not houses*/
SELECT * FROM zillow WHERE house_type <> 'House' limit 5;

/*11. Calculate average price per sqft for houses for sale grouping them by the number of bedrooms. */
select bedrooms, AVG(newprice) as average from zillow group by bedrooms;