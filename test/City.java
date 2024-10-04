package com.napier.devops;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class City {
    private String name;
    private String countryName;  // New field for Country Name
    private String district;
    private int population;

    // Constructor
    public City(String name, String countryName, String district, int population) {
        this.name = name;
        this.countryName = countryName;
        this.district = district;
        this.population = population;
    }

    // Getters
    public String getName() {
        return name;
    }

    public String getCountryName() {
        return countryName;
    }

    public String getDistrict() {
        return district;
    }

    public int getPopulation() {
        return population;
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

    // Function to print cities in a table format
    public static void printCities(List<City> cities, String header) {
        System.out.println("\n\n######## " + header + " ########");
        System.out.println("+-----------------+------------------+-----------------------------+------------+");
        System.out.printf("| %-15s | %-19s | %-27s | %-10s |\n",
                "City Name", "Country Name", "District", "Population");
        System.out.println("+-----------------+------------------+-----------------------------+------------+");

        for (City city : cities) {
            System.out.printf("| %-15s | %-19s | %-27s | %-10d |\n",
                    city.getName(), city.getCountryName(), city.getDistrict(), city.getPopulation());
        }

        System.out.println("+-----------------+------------------+-----------------------------+------------+");
    }
}
