--working on a POC to test iceberg merge running the statements below in sequence
create table ti ( 
	id int, 
	city String, 
	dept String 
)
STORED BY ICEBERG 
stored as orc 
tblproperties ('format-version'='2');

create table si ( 
	id int, 
	city String, 
	dept String 
)
STORED BY ICEBERG
stored as orc 
tblproperties ('format-version'='2');

INSERT INTO ti
VALUES 
(1, 'Deli', 'closed'),
(2, 'Mumbai', 'open'),
(3, 'Bangalore', 'open');

INSERT INTO si
VALUES 
(1, 'Deli', 'closed'),
(2, 'Mumbai', 'open'),
(3, 'BangaloreReplacement', 'open'), 
(4, 'Hyderabad', 'open');

merge into ti using si
ON ti.id = si.id
WHEN MATCHED AND ti.city = 'Bangalore' THEN UPDATE SET city = si.city
WHEN MATCHED AND ti.dept = 'closed' THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (si.id, '07', '2020')