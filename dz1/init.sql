CREATE DATABASE pets_db;
\c pets_db

CREATE TABLE IF NOT EXISTS raw_json (
    id SERIAL PRIMARY KEY,
    json_data JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_xml (
    id SERIAL PRIMARY KEY,
    xml_data TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flat_pets (
    id SERIAL PRIMARY KEY,
    name TEXT,
    species TEXT,
    fav_foods TEXT,
    birth_year INTEGER,
    photo TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS flat_foods (
    id SERIAL PRIMARY KEY,
    food_name TEXT,
    manufacturer TEXT,
    serving_size TEXT,
    calories_total INTEGER,
    calories_fat INTEGER,
    total_fat TEXT,
    saturated_fat TEXT,
    cholesterol TEXT,
    sodium TEXT,
    carb TEXT,
    fiber TEXT,
    protein TEXT,
    vitamin_a TEXT,
    vitamin_c TEXT,
    calcium TEXT,
    iron TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO raw_json (json_data) VALUES ('{
  "pets": [
    {
      "name": "Purrsloud",
      "species": "Cat",
      "favFoods": ["wet food", "dry food", "<strong>any</strong> food"],
      "birthYear": 2016,
      "photo": "https://learnwebcode.github.io/json-example/images/cat-2.jpg"
    },
    {
      "name": "Barksalot",
      "species": "Dog",
      "birthYear": 2008,
      "photo": "https://learnwebcode.github.io/json-example/images/dog-1.jpg"
    },
    {
      "name": "Meowsalot",
      "species": "Cat",
      "favFoods": ["tuna", "catnip", "celery"],
      "birthYear": 2012,
      "photo": "https://learnwebcode.github.io/json-example/images/cat-1.jpg"
    }
  ]
}');

INSERT INTO raw_xml (xml_data) VALUES ($$<?xml version="1.0"?>
<nutrition>
<daily-values>
	<total-fat units="g">65</total-fat>
	<saturated-fat units="g">20</saturated-fat>
	<cholesterol units="mg">300</cholesterol>
	<sodium units="mg">2400</sodium>
	<carb units="g">300</carb>
	<fiber units="g">25</fiber>
	<protein units="g">50</protein>
</daily-values>
<food>
	<name>Avocado Dip</name>
	<mfr>Sunnydale</mfr>
	<serving units="g">29</serving>
	<calories total="110" fat="100"/>
	<total-fat>11</total-fat>
	<saturated-fat>3</saturated-fat>
	<cholesterol>5</cholesterol>
	<sodium>210</sodium>
	<carb>2</carb>
	<fiber>0</fiber>
	<protein>1</protein>
	<vitamins>
		<a>0</a>
		<c>0</c>
	</vitamins>
	<minerals>
		<ca>0</ca>
		<fe>0</fe>
	</minerals>
</food>
<food>
	<name>Bagels, New York Style </name>
	<mfr>Thompson</mfr>
	<serving units="g">104</serving>
	<calories total="300" fat="35"/>
	<total-fat>4</total-fat>
	<saturated-fat>1</saturated-fat>
	<cholesterol>0</cholesterol>
	<sodium>510</sodium>
	<carb>54</carb>
	<fiber>3</fiber>
	<protein>11</protein>
	<vitamins>
		<a>0</a>
		<c>0</c>
	</vitamins>
	<minerals>
		<ca>8</ca>
		<fe>20</fe>
	</minerals>
</food>
<food>
	<name>Beef Frankfurter, Quarter Pound </name>
	<mfr>Armitage</mfr>
	<serving units="g">115</serving>
	<calories total="370" fat="290"/>
	<total-fat>32</total-fat>
	<saturated-fat>15</saturated-fat>
	<cholesterol>65</cholesterol>
	<sodium>1100</sodium>
	<carb>8</carb>
	<fiber>0</fiber>
	<protein>13</protein>
	<vitamins>
		<a>0</a>
		<c>2</c>
	</vitamins>
	<minerals>
		<ca>1</ca>
		<fe>6</fe>
	</minerals>
</food>
<food>
	<name>Chicken Pot Pie</name>
	<mfr>Lakeson</mfr>
	<serving units="g">198</serving>
	<calories total="410" fat="200"/>
	<total-fat>22</total-fat>
	<saturated-fat>9</saturated-fat>
	<cholesterol>25</cholesterol>
	<sodium>810</sodium>
	<carb>42</carb>
	<fiber>2</fiber>
	<protein>10</protein>
	<vitamins>
		<a>20</a>
		<c>2</c>
	</vitamins>
	<minerals>
		<ca>2</ca>
		<fe>10</fe>
	</minerals>
</food>
<food>
	<name>Cole Slaw</name>
	<mfr>Fresh Quick</mfr>
	<serving units=" cup">1.5</serving>
	<calories total="20" fat="0"/>
	<total-fat>0</total-fat>
	<saturated-fat>0</saturated-fat>
	<cholesterol>0</cholesterol>
	<sodium>15</sodium>
	<carb>5</carb>
	<fiber>2</fiber>
	<protein>1</protein>
	<vitamins>
		<a>30</a>
		<c>45</c>
	</vitamins>
	<minerals>
		<ca>4</ca>
		<fe>2</fe>
	</minerals>
</food>
<food>
	<name>Eggs</name>
	<mfr>Goodpath</mfr>
	<serving units="g">50</serving>
	<calories total="70" fat="40"/>
	<total-fat>4.5</total-fat>
	<saturated-fat>1.5</saturated-fat>
	<cholesterol>215</cholesterol>
	<sodium>65</sodium>
	<carb>1</carb>
	<fiber>0</fiber>
	<protein>6</protein>
	<vitamins>
		<a>6</a>
		<c>0</c>
	</vitamins>
	<minerals>
		<ca>2</ca>
		<fe>4</fe>
	</minerals>
</food>
<food>
	<name>Hazelnut Spread</name>
	<mfr>Ferreira</mfr>
	<serving units="tbsp">2</serving>
	<calories total="200" fat="90"/>
	<total-fat>10</total-fat>
	<saturated-fat>2</saturated-fat>
	<cholesterol>0</cholesterol>
	<sodium>20</sodium>
	<carb>23</carb>
	<fiber>2</fiber>
	<protein>3</protein>
	<vitamins>
		<a>0</a>
		<c>0</c>
	</vitamins>
	<minerals>
		<ca>6</ca>
		<fe>4</fe>
	</minerals>
</food>
<food>
	<name>Potato Chips</name>
	<mfr>Lees</mfr>
	<serving units="g">28</serving>
	<calories total="150" fat="90"/>
	<total-fat>10</total-fat>
	<saturated-fat>3</saturated-fat>
	<cholesterol>0</cholesterol>
	<sodium>180</sodium>
	<carb>15</carb>
	<fiber>1</fiber>
	<protein>2</protein>
	<vitamins>
		<a>0</a>
		<c>10</c>
	</vitamins>
	<minerals>
		<ca>0</ca>
		<fe>0</fe>
	</minerals>
</food>
<food>
	<name>Soy Patties, Grilled</name>
	<mfr>Gardenproducts</mfr>
	<serving units="g">96</serving>
	<calories total="160" fat="45"/>
	<total-fat>5</total-fat>
	<saturated-fat>0</saturated-fat>
	<cholesterol>0</cholesterol>
	<sodium>420</sodium>
	<carb>10</carb>
	<fiber>4</fiber>
	<protein>9</protein>
	<vitamins>
		<a>0</a>
		<c>0</c>
	</vitamins>
	<minerals>
		<ca>0</ca>
		<fe>0</fe>
	</minerals>
</food>
<food>
	<name>Truffles, Dark Chocolate</name>
	<mfr>Lyndon's</mfr>
	<serving units="g">39</serving>
	<calories total="220" fat="170"/>
	<total-fat>19</total-fat>
	<saturated-fat>14</saturated-fat>
	<cholesterol>25</cholesterol>
	<sodium>10</sodium>
	<carb>16</carb>
	<fiber>1</fiber>
	<protein>1</protein>
	<vitamins>
		<a>0</a>
		<c>0</c>
	</vitamins>
	<minerals>
		<ca>0</ca>
		<fe>0</fe>
	</minerals>
</food>
</nutrition>$$);