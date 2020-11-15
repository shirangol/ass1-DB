--1.a
CREATE TABLE MediaItems(
    MID NUMBER(11,0) NOT NULL,
    TITLE VARCHAR2(200),
    PROD_YEAR NUMBER(4),
    TITLE_LENGTH NUMBER(5),
    CONSTRAINT MediaItems_pk PRIMARY KEY (MID)
);
--1.b
CREATE TABLE Similarity(
    MID1 NUMBER(11,0) NOT NULL,
    MID2 NUMBER(11,0) NOT NULL,
    SIMILARITY FLOAT,
    CONSTRAINT Similarity_pk PRIMARY KEY (MID1,MID2),
    CONSTRAINT MID1_fk FOREIGN KEY (MID1) REFERENCES MediaItems(MID),
    CONSTRAINT MID2_fk FOREIGN KEY (MID2) REFERENCES MediaItems(MID)
);

--1.c
CREATE OR REPLACE TRIGGER AutoIncrement BEFORE
INSERT ON MediaItems FOR EACH ROW
DECLARE 
    maxIndex NUMBER;
BEGIN
    SELECT MAX(MID) INTO maxIndex FROM MediaItems;
    IF maxIndex IS NULL THEN
        :new.MID:=0;
    ELSE
        :new.MID:= maxIndex+1;
    END IF;
    :new.TITLE_LENGTH:= LENGTH(:new.TITLE);
END;

--1.d
CREATE OR REPLACE FUNCTION MaximalDistance RETURN
NUMBER IS
    max_year NUMBER;
    min_year NUMBER;
    max_distance NUMBER;
BEGIN
    SELECT MAX(PROD_YEAR) INTO max_year FROM MediaItems;
    SELECT MIN(PROD_YEAR) INTO min_year FROM MediaItems;
    max_distance:= POWER((max_year-min_year),2);
    RETURN max_distance;
END  MaximalDistance;
    
--1.e
CREATE OR REPLACE FUNCTION SimCalculation(mid_1 NUMBER, mid_2 NUMBER, max_distance NUMBER) RETURN
FLOAT IS
    mid_1_year NUMBER;
    mid_2_year NUMBER;
    two_items_distance NUMBER;
    similarity FLOAT;
BEGIN
    SELECT PROD_YEAR INTO mid_1_year FROM MediaItems WHERE MID= mid_1;
    SELECT PROD_YEAR INTO mid_2_year FROM MediaItems WHERE MID= mid_2;    
    two_items_distance:= POWER((mid_1_year-mid_2_year),2);
    similarity:= 1-( two_items_distance/ max_distance);
    RETURN similarity;
END  SimCalculation;

