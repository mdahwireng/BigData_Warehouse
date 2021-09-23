import mysql.connector
myConnection = mysql.connector.connect( host='localhost', user='warehouse', passwd='password')
mycursor = myConnection.cursor()

script = """CREATE SCHEMA IF NOT EXISTS `Warehouse` ;
USE `Warehouse` ;


CREATE TABLE IF NOT EXISTS `Warehouse`.`stations` (
  `id` INT NOT NULL,
  `name` VARCHAR(225) NOT NULL,
  `lat` DECIMAL(6,5) NULL,
  `long` DECIMAL(6,5) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `Warehouse`.`weekdays` (
  `weekdays_id` INT NOT NULL,
  `day` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`weekdays_id`))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `Warehouse`.`traffic` (
  `station_id` INT NOT NULL,
  `weekday_id` INT NOT NULL,
  `hours` INT NOT NULL,
  `min` INT NOT NULL,
  `sec` INT NOT NULL,
  `tot_flow` VARCHAR(45) NOT NULL,
  INDEX `fk_weekdays_idx` (`weekdays_id` ASC) VISIBLE,
  INDEX `fk_station_idx` (`station_id` ASC) VISIBLE,
  CONSTRAINT `fk_station`
    FOREIGN KEY (`station_id`)
    REFERENCES `Warehouse`.`stations` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_weekdays`
    FOREIGN KEY (`weekday_id`)
    REFERENCES `Warehouse`.`weekdays` (`weekday_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;"""
mycursor.execute(script)
mycursor.close()