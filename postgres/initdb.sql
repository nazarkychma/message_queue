CREATE TABLE users (
    login VARCHAR(255) PRIMARY KEY,
    password VARCHAR(255) NOT NULL,
    is_blocked BOOLEAN DEFAULT false,
    can_produce BOOLEAN DEFAULT true,
    can_consume BOOLEAN DEFAULT true
);

insert into users (login, password, is_blocked, can_produce, can_consume) values ('test', '$2b$12$zGNFseguZc0ywdXRfv87d./vGgQM9ZSh0XtRNDFlSFry/PqcaSyeu', false, true, true);
insert into users (login, password, is_blocked, can_produce, can_consume) values ('test1', '$2b$12$zGNFseguZc0ywdXRfv87d./vGgQM9ZSh0XtRNDFlSFry/PqcaSyeu', false, false, true);
insert into users (login, password, is_blocked, can_produce, can_consume) values ('test2', '$2b$12$zGNFseguZc0ywdXRfv87d./vGgQM9ZSh0XtRNDFlSFry/PqcaSyeu', false, true, false);
insert into users (login, password, is_blocked, can_produce, can_consume) values ('test3', '$2b$12$zGNFseguZc0ywdXRfv87d./vGgQM9ZSh0XtRNDFlSFry/PqcaSyeu', true, true, true);