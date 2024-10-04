package com.napier.devops;

import java.sql.*;
import java.util.List;

public class App {
    private Connection con = null;

    public void connect() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load SQL driver.");
            System.exit(-1);
        }

        int retries = 10;
        for (int i = 0; i < retries; ++i) {
            System.out.println("Connecting to database...");
            try {
                Thread.sleep(2000);
                con = DriverManager.getConnection("jdbc:mysql://db:3306/world?useSSL=false&allowPublicKeyRetrieval=true", "root", "example");
                System.out.println("Successfully connected");
                break;
            } catch (SQLException sqle) {
                System.out.println("Failed to connect to the database (attempt " + (i + 1) + ")");
                System.out.println(sqle.getMessage());
            } catch (InterruptedException ie) {
                System.out.println("Thread interrupted? Should not happen.");
            }
        }
    }

    public void disconnect() {
        if (con != null) {
            try {
                con.close();
            } catch (Exception e) {
                System.out.println("Error closing connection to database.");
            }
        }
    }

    public static void main(String[] args) {
        // Create new Application instance
        App app = new App();

        // Connect to database
        app.connect();

        // ------------------------------------
        // Country data outputs
        // ------------------------------------

        // Display top 10 populated countries in the world
        List<Country> worldCountries = Country.getTop10WorldPopulatedCountries(app.con);
        Country.printCountries(worldCountries, "World Top 10 Largest to Smallest Populated Countries");

        // Display top 10 populated countries in Europe
        List<Country> europeCountries = Country.getTop10ContinentPopulatedCountries(app.con, "Europe");
        Country.printCountries(europeCountries, "Europe Top 10 Largest to Smallest Populated Countries");

        // Display top 10 populated countries in Southern Africa
        List<Country> southernAfricaCountries = Country.getTop10RegionPopulatedCountries(app.con, "Southern Africa");
        Country.printCountries(southernAfricaCountries, "Southern Africa Top 10 Largest to Smallest Populated Countries");

        // Display world population largest to smallest
        List<Country> allWorldCountries = Country.getWorldPopulationLargestToLowest(app.con);
        Country.printCountries(allWorldCountries, "World Population Largest to Smallest");

        // Display Asia's population largest to smallest
        List<Country> asiaCountries = Country.getContinentPopulationLargestToLowest(app.con, "Asia");
        Country.printCountries(asiaCountries, "Asia (Continent) Population Largest to Smallest");

        // Display Central America (Region) population largest to smallest
        List<Country> centralAmericaCountries = Country.getRegionPopulationLargestToLowest(app.con, "Central America");
        Country.printCountries(centralAmericaCountries, "Central America (Region) Population Largest to Smallest");

        // ------------------------------------
        // City data outputs
        // ------------------------------------

        // Display all cities in the world organized by population
        List<City> allWorldCities = City.getAllCitiesInWorld(app.con);
        City.printCities(allWorldCities, "All Cities in the World Organized by Population");

        // Display all cities in a continent organized by population (example: Europe)
        List<City> allContinentCities = City.getAllCitiesInContinent(app.con, "Europe");
        City.printCities(allContinentCities, "All Cities in Europe Organized by Population");

        // Display all cities in a region organized by population (example: Southern Africa)
        List<City> allRegionCities = City.getAllCitiesInRegion(app.con, "Southern Africa");
        City.printCities(allRegionCities, "All Cities in Southern Africa Organized by Population");

        // Display all cities in a country organized by population (example: USA)
        List<City> allCountryCities = City.getAllCitiesInCountry(app.con, "USA");
        City.printCities(allCountryCities, "All Cities in the USA Organized by Population");

        // Display all cities in a district organized by population (example: Texas)
        List<City> allDistrictCities = City.getAllCitiesInDistrict(app.con, "Texas");
        City.printCities(allDistrictCities, "All Cities in Texas Organized by Population");

        // Display top N populated cities in the world (example: top 10)
        List<City> topNCitiesWorld = City.getTopNCitiesInWorld(app.con, 10);
        City.printCities(topNCitiesWorld, "Top 10 Populated Cities in the World");

        // Display top N populated cities in a continent (example: top 5 in Europe)
        List<City> topNCitiesContinent = City.getTopNCitiesInContinent(app.con, "Europe", 10);
        City.printCities(topNCitiesContinent, "Top 5 Populated Cities in Europe");

        // Display top N populated cities in a region (example: top 5 in Southern Africa)
        List<City> topNCitiesRegion = City.getTopNCitiesInRegion(app.con, "Southern Africa", 10);
        City.printCities(topNCitiesRegion, "Top 5 Populated Cities in Southern Africa");

        // Display top N populated cities in a country (example: top 5 in USA)
        List<City> topNCitiesCountry = City.getTopNCitiesInCountry(app.con, "USA", 10);
        City.printCities(topNCitiesCountry, "Top 5 Populated Cities in the USA");

        // Display top N populated cities in a district (example: top 3 in Texas)
        List<City> topNCitiesDistrict = City.getTopNCitiesInDistrict(app.con, "Texas", 10);
        City.printCities(topNCitiesDistrict, "Top 3 Populated Cities in Texas");


        // Disconnect from the database
        app.disconnect();
    }
}
