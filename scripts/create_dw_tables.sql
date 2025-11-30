DROP TABLE IF EXISTS transaction_fact;
DROP TABLE IF EXISTS dim_category;
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_payment;

CREATE TABLE dim_date (
    date_id         BIGINT PRIMARY KEY,
    year            INT,
    quarter         INT,
    month           INT,
    weekday         VARCHAR,
    day             INT,
    hour            INT,
    minute          INT
);

CREATE TABLE dim_user (
    user_id         VARCHAR PRIMARY KEY,
    name            VARCHAR,
    address         VARCHAR,
    phone_number    VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    email           VARCHAR
);

CREATE TABLE dim_category (
    category_id     BIGINT PRIMARY KEY,
    category_type   VARCHAR,
    merchant        VARCHAR
);

CREATE TABLE dim_payment (
    payment_id          BIGINT PRIMARY KEY,
    payment_type        VARCHAR,
    payment_currency    VARCHAR,
    payment_method      VARCHAR
);

CREATE TABLE transaction_fact (
    transaction_id      VARCHAR PRIMARY KEY,
    category_id         BIGINT NOT NULL,
    date_id             BIGINT NOT NULL,
    user_id             VARCHAR NOT NULL,
    payment_id          BIGINT NOT NULL,
    transaction_amount  NUMERIC(18,2) NOT NULL,

    FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id)
);