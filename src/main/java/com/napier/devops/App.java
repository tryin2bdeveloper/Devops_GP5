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

            // Populate city list
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

    public static void main(String[] args) {
        // Create new Application instance
        App app = new App();
        // Connect to database
        app.connect();
      // Disconnect from the database
        app.disconnect();
    }
}