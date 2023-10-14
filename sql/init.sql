DROP TABLE IF EXISTS  pokemon.pokemon;
GO
DROP SCHEMA IF EXISTS pokemon;
GO
CREATE SCHEMA pokemon;
GO
CREATE TABLE pokemon.pokemon
(
    name varchar(255),
    hp int,
    evolved_from varchar(255)
);
-- not every statement needs to be separated by GO
insert into pokemon.pokemon values('Wartortle', 4, 'Squirtle')
GO
