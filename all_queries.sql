--------------------------------------[LIBRARY MANAGEMENT SYSTEM]---------------------------------------
CREATE DATABASE library_db;

--------------------------------------------------------------------------------------------------------
------------------------------------- CREATING TABLES --------------------------------------------------
--------------------------------------------------------------------------------------------------------
--location--
CREATE TABLE country(
	country_id bigserial CONSTRAINT country_pkey PRIMARY KEY,
	code varchar(3) NOT NULL UNIQUE,
	country varchar(50) NOT NULL UNIQUE
);
CREATE TABLE city(
	city_id bigserial CONSTRAINT city_pkey PRIMARY KEY,
	city varchar(50) NOT NULL,
	country_id integer REFERENCES country(country_id)
);
CREATE TABLE address(
	address_id bigserial CONSTRAINT address_pkey PRIMARY KEY,
	address text,
	postal_code varchar(10),
	city_id integer REFERENCES city(city_id)
);

--student info--
CREATE TABLE department(
	department_id integer CONSTRAINT dep_pkey PRIMARY KEY,
	department_name text,
	num_people integer    -- no. people in the department
);

-- book publishing details--
CREATE TABLE author (
	author_id bigserial CONSTRAINT author_pkey PRIMARY KEY,
	first_name varchar(50),   
	last_name varchar(50) 
);

CREATE TABLE publisher (
	pub_id bigserial CONSTRAINT publisher_pkey PRIMARY KEY,
	pub_name varchar(100) UNIQUE
);

-- book details --
CREATE TABLE category (
	cat_id bigserial CONSTRAINT category_pkey PRIMARY KEY,
	cat_name varchar(50) NOT NULL UNIQUE
);

CREATE TABLE languages (
	lang_id bigserial CONSTRAINT languages_pkey PRIMARY KEY,
	lang_name varchar(100) NOT NULL UNIQUE
);

CREATE TABLE shelf(
	shelf_num integer CONSTRAINT shelf_pkey PRIMARY KEY,
	floor_num integer NOT NULL
);

-- tables storing information about people (students and staff) --
CREATE TABLE customer (
	cust_id bigserial CONSTRAINT customer_pkey PRIMARY KEY,
	first_name varchar(50) NOT NULL,
	last_name varchar(50) NOT NULL,
	student_id integer UNIQUE NOT NULL,
	address_id integer REFERENCES address(address_id),
	department_id integer REFERENCES department(department_id)
);

CREATE TABLE staff (
	staff_id bigserial CONSTRAINT staff_pkey PRIMARY KEY,
	first_name varchar(50) NOT NULL,
	last_name varchar(50) NOT NULL,
	address_id integer REFERENCES address(address_id)
);

-- book (books, copies, book authors)--
CREATE TABLE book (
	book_id integer CONSTRAINT book_pkey PRIMARY KEY,
	title varchar(100) NOT NULL,
	publisher_id integer REFERENCES publisher(pub_id),
	category_id integer REFERENCES category(cat_id),
	language_id integer REFERENCES languages(lang_id)
);

CREATE TABLE book_author (   --so that any book can have multiple authors
	book_id bigserial REFERENCES book(book_id),
	author_id integer REFERENCES author(author_id)
);

CREATE TABLE book_copy (
	book_id integer REFERENCES book(book_id),
	copy_id text UNIQUE NOT NULL,
	shelf_num integer REFERENCES shelf(shelf_num),
	available varchar(3) NOT NULL,
	CONSTRAINT copy_book_pkey PRIMARY KEY (book_id, copy_id),
	CONSTRAINT check_available_in_list CHECK (available IN ('Yes', 'No')) 
);

-- operations (loan and reservation histories) --
CREATE TABLE loan (   
	staff_id integer REFERENCES staff(staff_id),
	customer_id integer REFERENCES customer(cust_id),
	book_copy_id text REFERENCES book_copy(copy_id),
	start_date date NOT NULL,
	returned_date date
);

CREATE TABLE reservation (
	staff_id integer REFERENCES staff(staff_id),
	customer_id integer REFERENCES customer(cust_id),
	book_id integer REFERENCES book(book_id),
	reservation_date timestamp NOT NULL,
	status varchar(10) NOT NULL,
	CONSTRAINT check_status_in_list CHECK (status IN ('Waiting', 'Fulfilled', 'Cancelled'))
);

--------------------------------------------------------------------------------------------------------
------------------------------------- INSERT VALUES INTO TABLES ----------------------------------------
--------------------------------------------------------------------------------------------------------

--------- fill in country, city, address tables -----------
--Source: https://simplemaps.com/data/world-cities --

CREATE TEMPORARY TABLE worldcities_temp (
	city text, city_ascii text, lat text, lng text, country text, 
	iso2 text, iso3 text, admin_name text, capital text, population text, id_ text);

COPY worldcities_temp
FROM '[YOUR_PATH]\worldcities.csv'
WITH (FORMAT CSV, HEADER);

DELETE FROM worldcities_temp
WHERE country = 'South Georgia And South Sandwich Islands';  -- DUPLICATED VALUE.

-- insert countries --
INSERT INTO country (code, country)
SELECT DISTINCT iso3, country FROM worldcities_temp;

-- insert cities --
INSERT INTO city (city, country_id)
SELECT DISTINCT worldcities_temp.city_ascii, country.country_id
FROM worldcities_temp
JOIN country ON worldcities_temp.country = country.country;

-- insert sample addresses --
WITH cities AS(
	WITH id_span AS ( SELECT max(city_id) - min(city_id) AS span FROM city )
	SELECT city_id, city, country_id, id_span.span AS span
	FROM city CROSS JOIN id_span
)
INSERT INTO address(city_id)
SELECT city_id FROM cities
WHERE random() < 0.1 limit 2000;

DROP TABLE worldcities_temp;

---------- fill in shelf table ------------

INSERT INTO shelf
SELECT num_shelf,
	   CASE WHEN num_shelf < 11 THEN 1
		    ELSE 2
	   END AS floor_num
FROM (SELECT generate_series(1,20) AS num_shelf) AS shelves;

--------- fill in department and category tables ----------
-- Source: https://github.com/fivethirtyeight/data/blob/master/college-majors/majors-list.csv --

CREATE TEMPORARY TABLE majors_list (f0d1p text, major text, major_category text);

COPY majors_list
FROM '[YOUR_PATH]\majors-list.csv'
WITH (FORMAT CSV, HEADER);

DELETE FROM majors_list
WHERE major_category = 'NA';  -- delete N/A (less than bachelor's degree) category.

CREATE OR REPLACE FUNCTION random_between(low int, high int)
RETURNS int AS
$$
BEGIN
	RETURN floor(random()*(high-low+1)+low);
END;
$$ language 'plpgsql' STRICT;

-- insert departments --
INSERT INTO department
SELECT f0d1p::int, major, random_between(200,800)
FROM majors_list;

-- insert into categories --
INSERT INTO category (cat_name)
SELECT DISTINCT major_category
FROM majors_list;

DROP TABLE majors_list;

-- add to each category a shelf number where a book of such category can be found --
ALTER TABLE category ADD COLUMN shelf_num integer REFERENCES shelf(shelf_num);

WITH shelf_span AS ( 
	SELECT max(shelf_num) AS max_value,
 		   min(shelf_num) AS min_value 
    FROM shelf 
)
UPDATE category
SET shelf_num = subquery.shelf_num
FROM (
	SELECT cat.cat_id, 
	   	   random_between(ss.min_value, ss.max_value) AS shelf_num
	FROM category AS cat CROSS JOIN shelf_span AS ss
) subquery
WHERE category.cat_id = subquery.cat_id;

------------ fill in language ---------------

CREATE TEMPORARY TABLE langs_list (col1 text, col2 text, col3 text, lang text, native_name text);
-- Reference: https://github.com/forxer/languages-list/blob/master/src/Languages.csv--

COPY langs_list
FROM '[YOUR_PATH]\languages-list.csv'
WITH (FORMAT CSV, HEADER);

INSERT INTO languages (lang_name)
SELECT DISTINCT trim(lang)
FROM langs_list;

DROP TABLE langs_list;

-------- fill in publisher, author ---------
--Source: https://gist.github.com/jaidevd/23aef12e9bf56c618c41 --

CREATE TEMPORARY TABLE books_list (title text, author text, genre text, subgenre text, height int, publisher text);

COPY books_list
FROM '[YOUR_PATH]\books-list.csv'
WITH (FORMAT CSV, HEADER);

INSERT INTO publisher (pub_name)
SELECT DISTINCT publisher
FROM books_list;

---- inspect book_list ------
-- different spellings for Deshpande, P L & Deshpande P L; Naipaul, V S& Naipaul, V. S. || Wells, H G & Wells, H. G.
-- SELECT author, count(*) AS author_count
-- FROM books_list
-- GROUP BY author
-- ORDER BY author ASC;

-- create a column copy and make changes there
ALTER TABLE books_list ADD COLUMN author_edited text;
UPDATE books_list SET author_edited = author;

UPDATE books_list
SET author_edited = 'Deshpande, P. L.'
WHERE author = 'Deshpande, P L' OR author = 'Deshpande P L';

UPDATE books_list
SET author_edited = 'Wells, H. G.'
WHERE author LIKE 'Wells%';

UPDATE books_list
SET author_edited = 'Naipaul, V. S.'
WHERE author LIKE 'Naipaul%';

-- The last and first name are not separated by comma, and order is reversed
UPDATE books_list
SET author_edited = 'Gutierrez, Sebastian'
WHERE author = 'Sebastian Gutierrez';

UPDATE books_list 
SET author = author_edited;
ALTER TABLE books_list DROP COLUMN author_edited;

-- Two identical books from the same author from 'Random House' and 'null' publisher with different height.
-- SELECT * FROM books_list
-- WHERE title = 'Angels & Demons'

DELETE FROM books_list
WHERE title = 'Angels & Demons' AND publisher IS NULL;

-- insert authors --
CREATE TEMPORARY TABLE authors_transformed(author text, last_name text, first_name_edited text);

WITH unique_authors(author, last_name, first_name, first_name_p1, first_name_p2) AS
	(SELECT 
		DISTINCT ON (author) author,
		split_part(author, ', ', 1) AS last_name,
		split_part(author, ', ', 2) AS first_name,
		split_part(split_part(author, ', ', 2), ' ', 1) AS first_name_p1,
		split_part(split_part(author, ', ', 2), ' ', 2) AS first_name_p2
	FROM books_list
	-- WHERE author IS NOT NULL
	)
INSERT INTO authors_transformed (author, last_name, first_name_edited)
SELECT author, 
	   last_name,
	   first_name_edited
FROM (
	SELECT author, 
		   last_name,
	       CASE WHEN char_length(first_name_p2) = 0 THEN first_name  -- no middle name -> first_name
	   			WHEN char_length(first_name_p2) > 1 THEN first_name  -- first_name, no need for periods, because is either full first and middle names or already has periods.
				WHEN char_length(first_name_p1) > 2 AND char_length(first_name_p2) = 1 THEN concat(first_name_p1, ' ', first_name_p2, '.')
	   			WHEN author IS NULL THEN NULL
				ELSE concat(first_name_p1, '. ', first_name_p2, '.')
	  		END AS first_name_edited
	FROM unique_authors
) AS unique_authors_transformed;

INSERT INTO author (first_name, last_name)
SELECT first_name_edited, last_name FROM authors_transformed;

------------ Get books ready for filling in book and book_author tables ------------

CREATE TEMPORARY TABLE books_collection(title text, last_name varchar(15), first_name varchar(15), 
										publisher varchar(30), genre varchar(20), subgenre varchar(20));

INSERT INTO books_collection
SELECT title, aut.last_name, aut.first_name_edited, bl.publisher, bl.genre, bl.subgenre
FROM books_list AS bl LEFT JOIN authors_transformed AS aut
ON bl.author = aut.author;

DROP TABLE authors_transformed;
DROP TABLE books_list;

-- edit category of the book based on genre and subgenre --
ALTER TABLE books_collection ADD COLUMN category varchar(60);

UPDATE books_collection
SET category = 'Physical Sciences'
WHERE genre = 'science' or subgenre= 'science';

UPDATE books_collection
SET category = 'Computers & Mathematics'
WHERE genre = 'tech' or subgenre = 'mathematics';

UPDATE books_collection
SET category = 'Business'
WHERE subgenre = 'economics';

UPDATE books_collection
SET category = 'Humanities & Liberal Arts'
WHERE genre = 'fiction' OR subgenre IN ('autobiography','anthology','poetry','history', 'trivia', 'philosophy',
									    'objectivism', 'misc');

UPDATE books_collection
SET category = 'Education'
WHERE subgenre = 'education';

UPDATE books_collection
SET category = 'Psychology & Social Work'
WHERE subgenre IN ('psychology', 'politics');

UPDATE books_collection
SET category = 'Law & Public Policy'
WHERE subgenre = 'legal';

ALTER TABLE books_collection DROP COLUMN genre;
ALTER TABLE books_collection DROP COLUMN subgenre;

-- edit language in which book is written --
ALTER TABLE books_collection ADD COLUMN lang varchar(100);

UPDATE books_collection
SET lang = 'English';

UPDATE books_collection
SET lang = 'German'
WHERE title = 'Mein Kampf';

UPDATE books_collection
SET lang = 'Marathi (Marāṭhī)'
WHERE last_name = 'Deshpande';

-- add the id of the book --
ALTER TABLE books_collection ADD COLUMN book_id bigserial;

-- insert books from book_collection to book table --
INSERT INTO book
SELECT bc.book_id, bc.title, pub.pub_id, cat.cat_id, langs.lang_id
FROM books_collection AS bc
LEFT JOIN publisher AS pub ON bc.publisher = pub.pub_name
LEFT JOIN category AS cat ON bc.category = cat.cat_name
JOIN languages AS langs ON bc.lang = langs.lang_name;

-- insert book ids and author ids into book_author --
INSERT INTO book_author
SELECT bc.book_id, au.author_id
FROM books_collection AS bc
LEFT JOIN author AS au ON bc.last_name = au.last_name AND bc.first_name = au.first_name;

DROP TABLE books_collection;

---------------- fill in staff table ----------
--Source: https://www.slingacademy.com/article/employees-sample-data/ --

CREATE TEMPORARY TABLE people_list (first_name text, last_name text, email text, phone text, gender text,
								    department text, job_title text, experience text, salary text);

COPY people_list
FROM '[YOUR_PATH]\employees.csv'
WITH (FORMAT CSV, HEADER);

ALTER TABLE people_list ADD COLUMN address_id integer;
ALTER TABLE people_list ADD column ppl_id bigserial;

-- randomly assign addresses from address table, which contains sample addresses --
UPDATE people_list
SET address_id = floor(random() * (max_value - min_value + 1)) + min_value
FROM ( SELECT min(address_id) as min_value, max(address_id) as max_value FROM address ) subquery;

INSERT INTO staff(first_name, last_name, address_id)
SELECT first_name, last_name, address_id
FROM people_list
WHERE people_list IS NOT NULL AND ppl_id BETWEEN 1 AND 50;

------------ fill in customer table -------------
						
ALTER TABLE people_list ADD COLUMN department_id integer;

-- assign random department_id from department table --
UPDATE people_list
SET department_id = subquery.department_id
FROM (
	SELECT p.ppl_id, d.department_id,
		   ROW_NUMBER() OVER (PARTITION BY p.ppl_id ORDER BY random()) as row_number
	FROM people_list p
	CROSS JOIN department d
) subquery
WHERE people_list.ppl_id = subquery.ppl_id AND subquery.row_number = 1;

ALTER TABLE people_list ADD COLUMN student_id INT UNIQUE;

-- Since we want to randomly generate unique student_ids, we'll use a loop and check for 
-- existing duplicates before assigning values.
DO $$
DECLARE
	row record;
	unique_id integer;
BEGIN
	FOR row IN SELECT ppl_id FROM people_list LOOP
		LOOP
			unique_id := random_between(1000, 50000);
			EXIT WHEN NOT EXISTS (SELECT 1 FROM people_list WHERE student_id = unique_id);
		END LOOP;

		UPDATE people_list 
		SET student_id = unique_id
		WHERE ppl_id = row.ppl_id;
	END LOOP;
END $$;


INSERT INTO customer(first_name, last_name, student_id, address_id, department_id)
SELECT first_name, last_name, student_id, address_id, department_id
FROM people_list
WHERE people_list IS NOT NULL AND ppl_id > 50;

DROP TABLE people_list;


-------- fill in book_copy table ---------

INSERT INTO book_copy(book_id, copy_id, shelf_num, available)
SELECT
  book_id,
  concat(book_id, '-', generate_series(1, random_between(1,3))) AS copy_id,
  category.shelf_num,
  CASE WHEN RANDOM() < 0.3 THEN 'Yes' ELSE 'No' END AS available
FROM book
LEFT JOIN category ON book.category_id = category.cat_id
ORDER BY book.book_id;

----------- fill in loan table with previous and active borrows ------------

CREATE TEMPORARY TABLE borrows_temp (staff_id integer, customer_id integer, book_copy_id text, 
									 start_date date, returned_date date);
									 
INSERT INTO borrows_temp (book_copy_id, start_date, returned_date)
SELECT copy_id,
	   date(NOW() - random()* INTERVAL '30 days') AS start_date,
	   NULL as returned_date  -- not available, so hasn't been returned yet.
FROM book_copy 
WHERE available = 'No';

-- create a few previous loans, which are available by now.
INSERT INTO borrows_temp (book_copy_id, start_date)
SELECT copy_id,
	   date(NOW() - random()* INTERVAL '60 days') AS start_date
FROM book_copy 
WHERE available = 'Yes'
LIMIT 30;

-- generate returned date for returned books (previous loans).
UPDATE borrows_temp
SET returned_date = subquery.returned_date
FROM (
	SELECT borrows_temp.book_copy_id,
		   (start_date + (random() * (now() - start_date)))::date AS returned_date
	FROM borrows_temp LEFT JOIN book_copy on borrows_temp.book_copy_id = book_copy.copy_id
	WHERE available = 'Yes'
) subquery
WHERE borrows_temp.book_copy_id = subquery.book_copy_id;


-- assign staff randomly
UPDATE borrows_temp
SET staff_id = subquery.staff_id
FROM (
	SELECT bt.book_copy_id, s.staff_id,
		   ROW_NUMBER() OVER (PARTITION BY bt.book_copy_id ORDER BY random()) as row_number
	FROM borrows_temp bt
	CROSS JOIN staff s
) subquery
WHERE borrows_temp.book_copy_id = subquery.book_copy_id AND subquery.row_number = 1;

-- assign customers randomly
UPDATE borrows_temp
SET customer_id = subquery.cust_id
FROM (
	SELECT bt.book_copy_id, cu.cust_id,
		   ROW_NUMBER() OVER (PARTITION BY bt.book_copy_id ORDER BY random()) as row_number
	FROM borrows_temp bt
	CROSS JOIN customer as cu
) subquery
WHERE borrows_temp.book_copy_id = subquery.book_copy_id AND subquery.row_number = 1;

-- insert everything in the loan table --
INSERT INTO loan(staff_id, customer_id, book_copy_id, start_date, returned_date)
SELECT staff_id, customer_id, book_copy_id, start_date, returned_date
FROM borrows_temp;

ALTER TABLE loan ADD COLUMN loan_id bigserial CONSTRAINT loan_id_pkey PRIMARY KEY; 
DROP TABLE borrows_temp;

---------- fill in reservation table -----------

CREATE TEMPORARY TABLE res_temp (staff_id integer, customer_id integer, book_id integer, 
								 reservation_date timestamp, status text);

-- retrieve only the book_id values of book copies where none of the copies are available
-- and make it possible for a couple of reservations to be made for the same book by different people.
INSERT INTO res_temp (book_id, reservation_date, status)
SELECT subquery.book_id,
	   (NOW() - random()* INTERVAL '40 days')::timestamp AS reservation_date,
	   'Waiting' AS status
FROM (
	SELECT bc.book_id, random_between(1,5) AS repetitions
	FROM book_copy bc
	WHERE NOT EXISTS (
	  SELECT 1
	  FROM book_copy
	  WHERE book_id = bc.book_id AND available = 'Yes'
	)
	GROUP BY bc.book_id
) subquery
CROSS JOIN generate_series(1, subquery.repetitions) AS gs(i);

ALTER TABLE res_temp ADD COLUMN res_id bigserial CONSTRAINT loan_id_pkey PRIMARY KEY;
							

INSERT INTO res_temp (book_id, reservation_date, status)
SELECT book_id,
	   (NOW() - random()* INTERVAL '60 days')::timestamp AS reservation_date,
	   'Fulfilled' AS status
FROM book_copy
WHERE available = 'Yes' AND random() < 0.4;


INSERT INTO res_temp (book_id, reservation_date, status)
SELECT book_id,
	   (NOW() - random()* INTERVAL '60 days')::timestamp AS reservation_date,
	   'Cancelled' AS status
FROM book_copy
WHERE random() < 0.05;


-- assign staff randomly
UPDATE res_temp
SET staff_id = subquery.staff_id
FROM (
	SELECT rt.res_id, s.staff_id,
		   ROW_NUMBER() OVER (PARTITION BY rt.res_id ORDER BY random()) as row_number
	FROM res_temp rt
	CROSS JOIN staff s
) subquery
WHERE res_temp.res_id = subquery.res_id AND subquery.row_number = 1;

-- assign customers randomly and ensure that the same customer cannot make 
-- multiple active ('Waiting') reservations for the same book.
UPDATE res_temp
SET customer_id = subquery.cust_id
FROM (
	SELECT rt.res_id, cu.cust_id,
		   ROW_NUMBER() OVER (PARTITION BY rt.res_id ORDER BY random()) as row_number
	FROM res_temp rt
	CROSS JOIN customer as cu
	WHERE NOT EXISTS (
		SELECT 1
		FROM res_temp AS r
		WHERE r.book_id = rt.book_id
		AND r.customer_id = cu.cust_id
		AND r.status = 'Waiting'
	)
) subquery
WHERE res_temp.res_id = subquery.res_id AND subquery.row_number = 1;


INSERT INTO reservation(staff_id, customer_id, book_id, reservation_date, status)
SELECT staff_id, customer_id, book_id, reservation_date, status
FROM res_temp;

DROP TABLE res_temp;

----------------------------------------------------------------------------------------------------------
-------------------------------------------- VIEWS -------------------------------------------------------
----------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW all_loans AS
SELECT loan.loan_id,
	   loan.start_date,
	   CASE WHEN loan.returned_date IS NOT NULL THEN (loan.returned_date)::text
	        ELSE 'None'
	   END AS returned_date,
	   book.book_id,
	   book.title,
	   concat(author.first_name, ' ', author.last_name) AS author_name,
	   book_copy.copy_id,
	   loan.customer_id,
	   concat(customer.first_name, ' ', customer.last_name) AS customer_name,	   
	   loan.staff_id, 
	   concat(staff.first_name, ' ', staff.last_name) AS staff_name
FROM loan
JOIN staff ON staff.staff_id = loan.staff_id
JOIN customer ON customer.cust_id = loan.customer_id
JOIN book_copy ON book_copy.copy_id = loan.book_copy_id
JOIN book ON book.book_id = book_copy.book_id
JOIN book_author ON book_author.book_id = book.book_id
JOIN author ON author.author_id = book_author.author_id
ORDER BY start_date DESC;

-- active loans
CREATE OR REPLACE VIEW active_loans AS
SELECT 
	   CASE WHEN NOW() - start_date > INTERVAL '14 days' THEN (-1) * (NOW()::date - start_date - 14)  -- if overdue
	   		ELSE 14 - (NOW()::date - start_date)  -- if still not overdue
	   END AS days_left,
	   book_id,
	   title,
	   copy_id,
	   customer_id,
	   customer_name,	   
	   staff_id, 
	   staff_name
FROM all_loans
WHERE returned_date = 'None'
ORDER BY days_left ASC;

-- all loans overdue: both previous and active overdues.
CREATE OR REPLACE VIEW all_overdue_loans AS
SELECT 
	   CASE WHEN returned_date != 'None' THEN (-1)* (returned_date::date - start_date - 14)  -- past overdues
	   		ELSE (-1)* (NOW()::date - start_date - 14)  -- active overdues
	   END AS overdue_by_days,
	   CASE WHEN returned_date != 'None' THEN 'Yes'
	        ELSE 'No'
	   END AS returned,
	   book_id,
	   title,
	   author_name,
	   copy_id,
	   customer_id,
	   customer_name,	   
	   staff_id, 
	   staff_name
FROM all_loans
WHERE (returned_date != 'None' AND (returned_date::date - start_date) > 14) OR 
      (returned_date = 'None' AND NOW()::date - start_date > 14 )
ORDER BY overdue_by_days ASC;

-- only active loans overdue: show only overdue books that havent been returned.
CREATE OR REPLACE VIEW active_overdue_loans AS
SELECT overdue_by_days, book_id, title, author_name, copy_id,
	   customer_id, customer_name, staff_id, staff_name
FROM all_overdue_loans
WHERE returned = 'No';

-- books that are not available (none of the copies are available).
CREATE OR REPLACE VIEW unavailable_books AS
SELECT subquery.book_id, 
	   book.title, 
	   concat(author.first_name, ' ', author.last_name) AS author,
	   category.cat_name AS category,
	   languages.lang_name AS language,
	   publisher.pub_name AS publisher
FROM (
	SELECT bc.book_id
	FROM book_copy bc
	WHERE NOT EXISTS (
		SELECT 1
		FROM book_copy
		WHERE book_id = bc.book_id AND available = 'Yes'
	)
	GROUP BY bc.book_id
) subquery
JOIN book ON subquery.book_id = book.book_id
JOIN book_author ON book_author.book_id = book.book_id
LEFT JOIN author ON author.author_id = book_author.author_id
JOIN category ON category.cat_id = book.category_id
JOIN languages ON languages.lang_id = book.language_id
LEFT JOIN publisher ON publisher.pub_id = book.publisher_id;


------------------------------------------------------------------------------------------------------------------
------------------------------PROCEDURES (INSERT, UPDATE, DELETE)-------------------------------------------------
------------------------------------------------------------------------------------------------------------------

-- Staff: insert
CREATE OR REPLACE PROCEDURE InsertStaff(_first_name varchar(50), _last_name varchar(50), 
										_city varchar(50), _address text, _postal_code varchar(10))
LANGUAGE plpgsql
AS $$
DECLARE 
	_city_id integer;
	_address_id integer;
BEGIN

	-- Get the city_id based on the provided city name
	SELECT city_id INTO _city_id
	FROM city
	WHERE city = _city;
	
	IF _city_id IS NULL THEN
		RAISE EXCEPTION 'City not found';
	END IF;
	
	-- Check if address already exists
	SELECT address_id INTO _address_id
	FROM address
	WHERE city_id = _city_id 
		  AND (address = _address OR (address IS NULL AND _address IS NULL))
		  AND (postal_code = _postal_code OR (postal_code IS NULL AND _postal_code IS NULL))
	LIMIT 1;
	
	-- If the address doesn't exist, insert a new address
	IF _address_id IS NULL THEN
		INSERT INTO address(address, postal_code, city_id)
		VALUES (_address, _postal_code, _city_id)
		RETURNING address_id INTO _address_id;
	END IF;

	-- Insert into the staff table using the retrieved city_id and address_id
	INSERT INTO staff(first_name,last_name,address_id)
	VALUES (_first_name, _last_name, _address_id);
	
END; 
$$;

CALL InsertStaff('Olivia','Mein','Laren', null, null);
CALL InsertStaff('Hanz','Mein','Laren', null, null);
CALL InsertStaff('Jay','Black','Dom Pedro','Rua São Sebastião, 123', '65430-000');

-- Customer: insert
CREATE OR REPLACE PROCEDURE InsertCustomer(_first_name varchar(50), _last_name varchar(50), _student_id integer,
										   _city varchar(50), _address text, _postal_code varchar(10),
										   _department_name text)
LANGUAGE plpgsql
AS $$
DECLARE 
	__student_id integer;
	_city_id integer;
	_address_id integer;
	_department_id integer;
BEGIN
	-- Each student is supposed to have an unique student_id. Check whether this student_id already exists in the table.
	SELECT student_id INTO __student_id
	FROM customer
	WHERE student_id = _student_id;
	
	IF __student_id IS NOT NULL THEN
		RAISE EXCEPTION 'The student has already been added to the table.';
	END IF;

	-- Get the city_id based on the provided city name
	SELECT city_id INTO _city_id
	FROM city
	WHERE city = _city;
	
	IF _city_id IS NULL THEN
		RAISE EXCEPTION 'City not found';
	END IF;
	
	-- Check if address already exists
	SELECT address_id INTO _address_id
	FROM address
	WHERE city_id = _city_id 
		  AND (address = _address OR (address IS NULL AND _address IS NULL))
		  AND (postal_code = _postal_code OR (postal_code IS NULL AND _postal_code IS NULL))
	LIMIT 1;
	
	-- If the address doesn't exist, insert a new address into address table and retrieve new address_id
	IF _address_id IS NULL THEN
		INSERT INTO address(address, postal_code, city_id)
		VALUES (_address, _postal_code, _city_id)
		RETURNING address_id INTO _address_id;
	END IF;

	-- Get the department_id based on the provided department name
	SELECT department_id INTO _department_id
	FROM department
	WHERE department_name = _department_name;
	
	IF _department_id IS NULL THEN
		RAISE EXCEPTION 'Department not found';
	END IF;
	
	-- Insert into the customer table using the retrieved city_id, address_id, and department_id
	INSERT INTO customer(first_name,last_name,student_id,address_id,department_id)
	VALUES (_first_name, _last_name, _student_id, _address_id, _department_id);
	
END; 
$$;

CALL InsertCustomer('May','Hwang',45394, 'Bauchi', null, null, 'SCIENCE AND COMPUTER TEACHER EDUCATION');
CALL InsertCustomer('Joan','Ember',43589, 'New York', null, null, 'COGNITIVE SCIENCE AND BIOPSYCHOLOGY');

-- return loaned book (update loan and book_copy tables)
CREATE OR REPLACE PROCEDURE ReturnBook(_student_id integer, _book_copy_id text)
LANGUAGE plpgsql
AS $$
DECLARE 
	_customer_id integer;
BEGIN
	-- Retrieve customer_id from customer table based on student_id
	SELECT cust_id INTO _customer_id
	FROM customer
	WHERE student_id = _student_id;
	
	IF _customer_id IS NULL THEN
		RAISE EXCEPTION 'Student ID not found.';
	END IF;
	
	-- Check if the book copy exists in the loan table
	IF NOT EXISTS(SELECT 1 FROM loan 
				  WHERE returned_date IS NULL AND book_copy_id = _book_copy_id AND customer_id = _customer_id) THEN
		RAISE EXCEPTION 'No active loan is found for the given book copy and student ID';
	END IF;
	
	-- Update the returned date to the current date for the specific book copy and student ID
	UPDATE loan
	SET returned_date = CURRENT_DATE
	WHERE returned_date IS NULL AND book_copy_id = _book_copy_id AND customer_id = _customer_id;
	 
	-- Change the availability of the book to Yes in book_copy table.
	UPDATE book_copy
	SET available = 'Yes'
	WHERE copy_id = _book_copy_id;
END;
$$;

CALL ReturnBook(16605, '10-1');

-- Loan the Book (Insert into loan and update book_copy)
CREATE OR REPLACE PROCEDURE LoanBook(_student_id integer, _book_copy_id text, _staff_id integer)
LANGUAGE plpgsql
AS $$
DECLARE 
	__staff_id integer;
	_customer_id integer;
BEGIN
	-- Check if the book copy exists in the loan table and hasnt been returned, i.e. is already loaned.
	IF EXISTS(SELECT 1 FROM loan
			  WHERE returned_date IS NULL AND book_copy_id = _book_copy_id) THEN
		RAISE EXCEPTION 'Active loan is found for the given book copy';
	END IF;

	-- Retrieve customer_id from customer table based on student_id
	SELECT cust_id INTO _customer_id
	FROM customer
	WHERE student_id = _student_id;
	
	IF _customer_id IS NULL THEN
		RAISE EXCEPTION 'Student ID not found.';
	END IF;
	
	-- Retrieve staff_id from staff table
	SELECT staff_id INTO __staff_id
	FROM staff
	WHERE staff_id = _staff_id;
	
	IF __staff_id IS NULL THEN
		RAISE EXCEPTION 'Staff ID not found.';
	END IF;
	
	INSERT INTO loan(staff_id, customer_id, book_copy_id, start_date, returned_date)
	VALUES (_staff_id, _customer_id, _book_copy_id, CURRENT_DATE, null);
	 
	-- Change the 'available' status of the book to No in book_copy table.
	UPDATE book_copy
	SET available = 'No'
	WHERE copy_id = _book_copy_id;
END;
$$;

CALL LoanBook(45394, '10-1', 25);

-----------------------------------------------------------------------------------------------------------------
--------------------------------------------------- PROCEDURES --------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

-- Customer's active loans
CREATE OR REPLACE PROCEDURE ShowCustomerActiveLoans(_student_id integer)
LANGUAGE plpgsql
AS $$
DECLARE
	_customer_id integer;
	_loan_id loan.loan_id%TYPE;
	_start_date loan.start_date%TYPE;
	_days_left integer;
	_book_copy_id loan.book_copy_id%TYPE;
	_title book.title%TYPE;
	_author_name text;
	
	-- Declare cursor to hold the query results
	cur_loan CURSOR FOR
		SELECT loan_id, start_date,
			   CASE WHEN CURRENT_DATE - start_date > 14 THEN (-1) * (CURRENT_DATE - start_date - 14)  -- if overdue
	   		   		ELSE 14 - (CURRENT_DATE - start_date)  -- not overdue
	   		   END AS days_left,
			   book_copy_id, title, 
			   concat(author.first_name, ' ', author.last_name) AS author_name
		FROM loan
		JOIN book_copy ON loan.book_copy_id = book_copy.copy_id
		JOIN book ON book_copy.book_id = book.book_id
		JOIN book_author ON book_author.book_id = book.book_id
		JOIN author ON author.author_id = book_author.author_id
		WHERE customer_id = _customer_id AND returned_date IS NULL
		ORDER BY start_date;
BEGIN
	-- Retrieve customer_id from customer table based on student_id
	SELECT cust_id INTO _customer_id
	FROM customer
	WHERE student_id = _student_id;
	
	IF _customer_id IS NULL THEN
		RAISE EXCEPTION 'Student ID not found.';
	END IF;
	
	-- Open the cursor.
	OPEN cur_loan;
	
	-- Fetch and process each row from the cursor 
	LOOP 
		FETCH cur_loan INTO _loan_id, _start_date, _days_left, _book_copy_id, _title, _author_name;
		EXIT WHEN NOT FOUND;
		
		-- Print the loan details.
		RAISE INFO E'Loan ID: % \n|| Start date: % \n|| Days Left: % \n|| Book Copy ID: % \n|| Title: % \n|| Author: %.\n', 
					_loan_id, _start_date, _days_left, _book_copy_id, _title, _author_name;
	END LOOP;
	
	-- Close the cursor
	CLOSE cur_loan;
END;
$$;

CALL ShowCustomerActiveLoans(49439);

-- loan history of specific book
CREATE OR REPLACE PROCEDURE ShowBookLoanHistory(_book_id integer)
LANGUAGE plpgsql
AS $$
DECLARE
	__book_id integer; 
	_title book.title%TYPE;
	_author_name text;
	_book_copy_id loan.book_copy_id%TYPE;
	_student_id integer;
	_start_date loan.start_date%TYPE;
	_returned_date text;
	_end_date loan.start_date%TYPE;
	
	-- Declare cursor to hold the query result
	cur_book CURSOR FOR
		SELECT book_copy_id,
			   student_id,
			   start_date,
			   CASE WHEN returned_date IS NULL THEN '-'
			   		ELSE returned_date::text
			   END AS returned_date_text,
			   start_date + INTERVAL '14 days' AS end_date
		FROM book
		JOIN book_copy ON book.book_id = book_copy.book_id
		JOIN loan ON loan.book_copy_id = book_copy.copy_id
		JOIN customer ON customer.cust_id = loan.customer_id
		WHERE book.book_id = _book_id
		ORDER BY start_date;
BEGIN
	-- Check whether such book id exists.
	SELECT book_id INTO __book_id
	FROM book
	WHERE book_id = _book_id;
	
	IF __book_id IS NULL THEN
		RAISE EXCEPTION 'Book ID not found.';
	END IF;

	-- Retrieve title and author's name
	SELECT title, concat(author.first_name, ' ', author.last_name) AS author_name INTO _title, _author_name
	FROM book
	JOIN book_author ON book_author.book_id = book.book_id
	JOIN author ON author.author_id = book_author.author_id
	WHERE book.book_id = _book_id;
	
	RAISE INFO E'== ID: % == % - %', _book_id, _title, _author_name;

	-- Open the cursor.
	OPEN cur_book;
	
	-- Fetch and process each row from the cursor 
	LOOP 
		FETCH cur_book INTO _book_copy_id, _student_id, _start_date, _returned_date, _end_date;
		EXIT WHEN NOT FOUND;
		
		-- Print the loan details.
		RAISE INFO E' \n ||| Copy ID: %\n || Student: %\n || Start: %\n || End: % \n || Return: %', 
					_book_copy_id, _student_id, _start_date, _end_date, _returned_date;
	END LOOP;
	
	-- Close the cursor
	CLOSE cur_book;
END;
$$;

CALL ShowBookLoanHistory(10);