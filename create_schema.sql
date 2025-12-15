CREATE DATABASE IF NOT EXISTS life_science_lecture_10
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE life_science_lecture_10;

-- Tabelle 1: Quellen (Online-Datenbanken)
CREATE TABLE source_database (
    source_id      INT AUTO_INCREMENT PRIMARY KEY,
    name           VARCHAR(50) NOT NULL,
    url            VARCHAR(255),
    description    VARCHAR(255)
);

-- Tabelle 2: Organismus
CREATE TABLE organism (
    organism_id      INT AUTO_INCREMENT PRIMARY KEY,
    scientific_name  VARCHAR(255) NOT NULL,
    common_name      VARCHAR(255),
    taxonomy_id      INT,
    UNIQUE (taxonomy_id)
);

-- Tabelle 3: Proteine (aus UniProt)
CREATE TABLE protein (
    protein_id        INT AUTO_INCREMENT PRIMARY KEY,
    uniprot_id        VARCHAR(20) NOT NULL,
    protein_name      VARCHAR(255),
    gene_name         VARCHAR(100),
    sequence_length   INT,
    organism_id       INT,
    source_id         INT NOT NULL,
    UNIQUE (uniprot_id),
    CONSTRAINT fk_protein_organism
        FOREIGN KEY (organism_id)
        REFERENCES organism(organism_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
    CONSTRAINT fk_protein_source
        FOREIGN KEY (source_id)
        REFERENCES source_database(source_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);
