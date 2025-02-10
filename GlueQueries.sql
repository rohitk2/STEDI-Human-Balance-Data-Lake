--- Customer Landing --> Customer Trusted
SELECT * FROM customer_landing where sharewithresearchasofdate is not null;

---(Accelerometer Landing, Customer Trusted) --> Accelerometer Landing
SELECT DISTINCT accelerometer_landing.* 
FROM accelerometer_landing 
INNER JOIN customer_trusted 
ON accelerometer_landing.user = customer_trusted.email;

---(Accelerometer Trusted, Customer Trusted) --> Customer Curated
SELECT DISTINCT customer_trusted.* 
FROM accelerometer_trusted 
INNER JOIN customer_trusted 
ON accelerometer_trusted.user = customer_trusted.email;

---(Step Trainer Landing, Customer Curated) --> Step Trainer Trusted
SELECT step_trainer_landing.*
FROM 
    step_trainer_landing
INNER JOIN 
    customer_curated 
ON 
    customer_curated.serialnumber = step_trainer_landing.serialnumber;

---(Step Trainer Trusted, Accelerometer Trusted) --> Machine Learning Curated
SELECT accelerometer_trusted.user, step_trainer_trusted.*, accelerometer_trusted.x, accelerometer_trusted.y, accelerometer_trusted.z
FROM 
    step_trainer_trusted
INNER JOIN 
    accelerometer_trusted 
ON 
    accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime;