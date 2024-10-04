package com.napier.devops;

import java.sql.*;
import java.util.ArrayList;
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
                Thread.sleep(3000);
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

    // 1. Function to return all cities in the world organized by population (largest to smallest)
    public static List<City> getAllCitiesInWorld(Connection con) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "ORDER BY city.Population DESC";
        return getCityList(con, query);
    }

    // 2. Function to return all cities in a continent organized by population (largest to smallest)
    public static List<City> getAllCitiesInContinent(Connection con, String continent) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE country.Continent = '" + continent + "' ORDER BY city.Population DESC";
        return getCityList(con, query);
    }

    // 3. Function to return all cities in a region organized by population (largest to smallest)
    public static List<City> getAllCitiesInRegion(Connection con, String region) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE country.Region = '" + region + "' ORDER BY city.Population DESC";
        return getCityList(con, query);
    }

    // 4. Function to return all cities in a country organized by population (largest to smallest)
    public static List<City> getAllCitiesInCountry(Connection con, String countryCode) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE city.CountryCode = '" + countryCode + "' ORDER BY city.Population DESC";
        return getCityList(con, query);
    }

    // 5. Function to return all cities in a district organized by population (largest to smallest)
    public static List<City> getAllCitiesInDistrict(Connection con, String district) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE city.District = '" + district + "' ORDER BY city.Population DESC";
        return getCityList(con, query);
    }

    // 6. Function to return top N populated cities in the world
    public static List<City> getTopNCitiesInWorld(Connection con, int N) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "ORDER BY city.Population DESC LIMIT " + N;
        return getCityList(con, query);
    }

    // 7. Function to return top N populated cities in a continent
    public static List<City> getTopNCitiesInContinent(Connection con, String continent, int N) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE country.Continent = '" + continent + "' ORDER BY city.Population DESC LIMIT " + N;
        return getCityList(con, query);
    }

    // 8. Function to return top N populated cities in a region
    public static List<City> getTopNCitiesInRegion(Connection con, String region, int N) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE country.Region = '" + region + "' ORDER BY city.Population DESC LIMIT " + N;
        return getCityList(con, query);
    }

    // 9. Function to return top N populated cities in a country
    public static List<City> getTopNCitiesInCountry(Connection con, String countryCode, int N) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE city.CountryCode = '" + countryCode + "' ORDER BY city.Population DESC LIMIT " + N;
        return getCityList(con, query);
    }

    // 10. Function to return top N populated cities in a district
    public static List<City> getTopNCitiesInDistrict(Connection con, String district, int N) {
        String query = "SELECT city.Name, country.Name AS CountryName, city.District, city.Population " +
                "FROM city JOIN country ON city.CountryCode = country.Code " +
                "WHERE city.District = '" + district + "' ORDER BY city.Population DESC LIMIT " + N;
        return getCityList(con, query);
    }

    // Helper function to execute the query and return a list of cities
    private static List<City> getCityList(Connection con, String query) {
        List<City> cities = new ArrayList<>();
        try {
            Statement stmt = con.createStatement();
            ResultSet rset = stmt.executeQuery(query);

            // Populate city list (From Largest to Lowest)
            while (rset.next()) {
                String name = rset.getString("Name");
                String countryName = rset.getString("CountryName"); // Country name from joined country table
                String district = rset.getString("District");
                int population = rset.getInt("Population");

                City city = new City(name, countryName, district, population);
                cities.add(city);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return cities;
    }

    // Function to print cities in a table format
    public static void printCities(List<City> cities, String header) {
        System.out.println("\n\n######## " + header + " ########");
        System.out.println("+------------------------------------+-----------------------------------------+-----------------------------+------------+");
        System.out.printf("| %-34s | %-39s | %-27s | %-10s |\n",
                "City Name", "Country Name", "District", "Population");
        System.out.println("+------------------------------------+-----------------------------------------+-----------------------------+------------+");

        for (City city : cities) {
            System.out.printf("| %-34s | %-39s | %-27s | %-10s |\n",
                    city.getName(), city.getCountryName(), city.getDistrict(), city.getPopulation());
        }

        System.out.println("+------------------------------------+-----------------------------------------+-----------------------------+------------+");
    }


    // Function to return top 10 world populated countries
    public static List<Country> getTop10WorldPopulatedCountries(Connection con) {
        return getCountryList(con,
                "SELECT country.Code, country.Name, country.Continent, country.Region, country.Population, city.Name AS Capital " +
                        "FROM country JOIN city ON country.Capital = city.ID ORDER BY Population DESC LIMIT 10"
        );
    }

    // Function to return top 10 populated countries in a continent
    public static List<Country> getTop10ContinentPopulatedCountries(Connection con, String continent) {
        return getCountryList(con,
                "SELECT country.Code, country.Name, country.Continent, country.Region, country.Population, city.Name AS Capital " +
                        "FROM country JOIN city ON country.Capital = city.ID WHERE country.Continent = '" + continent + "' ORDER BY Population DESC LIMIT 10"
        );
    }

    // Function to return top 10 populated countries in a region
    public static List<Country> getTop10RegionPopulatedCountries(Connection con, String region) {
        return getCountryList(con,
                "SELECT country.Code, country.Name, country.Continent, country.Region, country.Population, city.Name AS Capital " +
                        "FROM country JOIN city ON country.Capital = city.ID WHERE country.Region = '" + region + "' ORDER BY Population DESC LIMIT 10"
        );
    }

    // Function to return all countries in a continent ordered by population
    public static List<Country> getContinentPopulationLargestToLowest(Connection con, String continent) {
        return getCountryList(con,
                "SELECT country.Code, country.Name, country.Continent, country.Region, country.Population, city.Name AS Capital " +
                        "FROM country JOIN city ON country.Capital = city.ID WHERE country.Continent = '" + continent + "' ORDER BY Population DESC"
        );
    }

    // Function to return all countries in a region ordered by population
    public static List<Country> getRegionPopulationLargestToLowest(Connection con, String region) {
        return getCountryList(con,
                "SELECT country.Code, country.Name, country.Continent, country.Region, country.Population, city.Name AS Capital " +
                        "FROM country JOIN city ON country.Capital = city.ID WHERE country.Region = '" + region + "' ORDER BY Population DESC"
        );
    }

    // Function to return all countries ordered by population (worldwide)
    public static List<Country> getWorldPopulationLargestToLowest(Connection con) {
        return getCountryList(con,
                "SELECT country.Code, country.Name, country.Continent, country.Region, country.Population, city.Name AS Capital " +
                        "FROM country JOIN city ON country.Capital = city.ID ORDER BY Population DESC"
        );
    }



    // Helper function to execute the query and return a list of countries
    private static List<Country> getCountryList(Connection con, String query) {
        List<Country> countries = new ArrayList<>();
        try {
            Statement stmt = con.createStatement();
            ResultSet rset = stmt.executeQuery(query);

            // Populate country list
            while (rset.next()) {
                String countryCode = rset.getString("Code");
                String countryName = rset.getString("Name");
                String continent = rset.getString("Continent");
                String region = rset.getString("Region");
                int population = rset.getInt("Population");
                String capital = rset.getString("Capital");

                Country country = new Country(countryCode, countryName, continent, region, population, capital);
                countries.add(country);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return countries;
    }

    // Function to print countries in a table format
    public static void printCountries(List<Country> countries, String header) {
        System.out.println("\n\n######## " + header + " ########");
        System.out.println("+--------------+-----------------------------------------+---------------+-----------------------------+------------+-----------------------------------+");
        System.out.printf("| %-12s | %-39s | %-13s | %-27s | %-10s | %-33s |\n",
                "Country Code", "Country Name", "Continent", "Region", "Population", "Capital");
        System.out.println("+--------------+-----------------------------------------+---------------+-----------------------------+------------+-----------------------------------+");

        for (Country country : countries) {
            System.out.printf("| %-12s | %-39s | %-13s | %-27s | %-10s | %-33s |\n",
                    country.getCountryCode(), country.getName(), country.getContinent(),
                    country.getRegion(), country.getPopulation(), country.getCapital());
        }

        System.out.println("+--------------+-----------------------------------------+---------------+-----------------------------+------------+-----------------------------------+");
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
        List<Country> worldCountries = getTop10WorldPopulatedCountries(app.con);
        printCountries(worldCountries, "World Top 10 Largest to Smallest Populated Countries");

        // Display top 10 populated countries in Europe
        List<Country> europeCountries = getTop10ContinentPopulatedCountries(app.con, "Europe");
        printCountries(europeCountries, "Europe Top 10 Largest to Smallest Populated Countries");

        // Display top 10 populated countries in Southern Africa
        List<Country> southernAfricaCountries = getTop10RegionPopulatedCountries(app.con, "Southern Africa");
        printCountries(southernAfricaCountries, "Southern Africa Top 10 Largest to Smallest Populated Countries");

        // Display world population largest to smallest
        List<Country> allWorldCountries = getWorldPopulationLargestToLowest(app.con);
        printCountries(allWorldCountries, "World Population Largest to Smallest");

        // Display Asia's population largest to smallest
        List<Country> asiaCountries = getContinentPopulationLargestToLowest(app.con, "Asia");
        printCountries(asiaCountries, "Asia (Continent) Population Largest to Smallest");

        // Display Central America (Region) population largest to smallest
        List<Country> centralAmericaCountries = getRegionPopulationLargestToLowest(app.con, "Central America");
        printCountries(centralAmericaCountries, "Central America (Region) Population Largest to Smallest");

        // ------------------------------------
        // City data outputs
        // ------------------------------------

        // Display all cities in the world organized by population
        List<City> allWorldCities = getAllCitiesInWorld(app.con);
        printCities(allWorldCities, "All Cities in the World Organized by Population");

        // Display all cities in a continent organized by population (example: Europe)
        List<City> allContinentCities = getAllCitiesInContinent(app.con, "Europe");
        printCities(allContinentCities, "All Cities in Europe Organized by Population");

        // Display all cities in a region organized by population (example: Southern Africa)
        List<City> allRegionCities = getAllCitiesInRegion(app.con, "Southern Africa");
        printCities(allRegionCities, "All Cities in Southern Africa Organized by Population");

        // Display all cities in a country organized by population (example: USA)
        List<City> allCountryCities = getAllCitiesInCountry(app.con, "USA");
        printCities(allCountryCities, "All Cities in the USA Organized by Population");

        // Display all cities in a district organized by population (example: Texas)
        List<City> allDistrictCities = getAllCitiesInDistrict(app.con, "Texas");
        printCities(allDistrictCities, "All Cities in Texas Organized by Population");

        // Display top N populated cities in the world (example: top 10)
        List<City> topNCitiesWorld = getTopNCitiesInWorld(app.con, 10);
        printCities(topNCitiesWorld, "Top 10 Populated Cities in the World");

        // Display top N populated cities in a continent (example: top 5 in Europe)
        List<City> topNCitiesContinent = getTopNCitiesInContinent(app.con, "Europe", 10);
        printCities(topNCitiesContinent, "Top 5 Populated Cities in Europe");

        // Display top N populated cities in a region (example: top 5 in Southern Africa)
        List<City> topNCitiesRegion = getTopNCitiesInRegion(app.con, "Southern Africa", 10);
        printCities(topNCitiesRegion, "Top 5 Populated Cities in Southern Africa");

        // Display top N populated cities in a country (example: top 5 in USA)
        List<City> topNCitiesCountry = getTopNCitiesInCountry(app.con, "USA", 10);
        printCities(topNCitiesCountry, "Top 5 Populated Cities in the USA");

        // Display top N populated cities in a district (example: top 3 in Texas)
        List<City> topNCitiesDistrict = getTopNCitiesInDistrict(app.con, "Texas", 10);
        printCities(topNCitiesDistrict, "Top 3 Populated Cities in Texas");


        // Disconnect from the database
        app.disconnect();
    }
}
