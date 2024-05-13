CREATE DATABASE logistic;


CREATE TABLE dim_location (
    RegionID INT AUTO_INCREMENT PRIMARY KEY,
    Region VARCHAR(255),
    CONSTRAINT unique_region UNIQUE (Region)
);

CREATE TABLE dim_customer (
    CustomerID VARCHAR(20) PRIMARY KEY,
    CustomerName VARCHAR(255) NOT NULL,
    Phone VARCHAR(15),
    Email VARCHAR(255),
    CONSTRAINT unique_email UNIQUE (Email)
);

CREATE TABLE dim_booking (
    BookingID VARCHAR(20) PRIMARY KEY,
    pickupDateTimeUTC DATETIME NOT NULL,
    pickupDateTimeLocal DATETIME NOT NULL,
    drivingDistanceInKm DECIMAL (10, 2),
    pickupLocation VARCHAR(255),
    dropOffLocation VARCHAR(255),
    flightNumber VARCHAR(20),
    bookingStatus VARCHAR(20),
	totalPrice DECIMAL (10, 2)
);

CREATE TABLE dim_vehicle (
	carID INT AUTO_INCREMENT PRIMARY KEY,
	carType VARCHAR (50) NOT NULL,
	CONSTRAINT unique_car UNIQUE (carType)
);


CREATE TABLE dim_time (
	TimeID INT AUTO_INCREMENT PRIMARY KEY,
	year INT NOT NULL,
	month INT NOT NULL,
	dayOfWeek INT NOT NULL
);

CREATE TABLE dim_supplier (
	supplierID INT AUTO_INCREMENT PRIMARY KEY,
	supplierType VARCHAR (50) NOT NULL,
	supplierName VARCHAR (50) NOT NULL,
	CONSTRAINT unique_supplier UNIQUE (supplierType, supplierName)
);


CREATE TABLE fact_sale (
	TotalVND DECIMAL (20, 2),
	TotalUSD DECIMAL (20, 2),
	PaymentVND DECIMAL (20, 2),
	PaymentUSD DECIMAL (20, 2),
	ProfitVND DECIMAL (20, 2),
	ProfitUSD DECIMAL (20, 2),
	SaleVND DECIMAL (20, 2),
	SaleUSD DECIMAL (20, 2),
	RegionID INT REFERENCES dim_location(RegionID),
	SupplierID INT REFERENCES dim_supplier(supplierID),
	CustomerID VARCHAR(20) REFERENCES dim_customer(CustomerID),
	TimeID DATE REFERENCES dim_time(TimeID),
	BookingID VARCHAR(20) REFERENCES dim_booking(BookingID),
	VehicleID INT REFERENCES dim_vehicle(carID),
	CONSTRAINT unique_fact_sale UNIQUE (RegionID, SupplierID, CustomerID, TimeID, BookingID, VehicleID)
);


CREATE TABLE driver_report (
	BookingID INT PRIMARY KEY REFERENCES fact_sale(BookingID),
	driverEventStatus VARCHAR(20),
	riderScore VARCHAR(10),
	incidentType VARCHAR(30)
);
