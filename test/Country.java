package com.napier.devops;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Country {
    private String countryCode;
    private String name;
    private String continent;
    private String region;
    private int population;
    private String capital;

    // Constructor
    public Country(String countryCode, String name, String continent, String region, int population, String capital) {
        this.countryCode = countryCode;
        this.name = name;
        this.continent = continent;
        this.region = region;
        this.population = population;
        this.capital = capital;
    }

    // Getters
    public String getCountryCode() {
        return countryCode;
    }

    public String getName() {
        return name;
    }

    public String getContinent() {
        return continent;
    }

    public String getRegion() {
        return region;
    }

    public int getPopulation() {
        return population;
    }

    public String getCapital() {
        return capital;
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
        System.out.println("+-----------------+------------------+------------+-----------------------------+------------+-------------------+");
        System.out.printf("| %-15s | %-19s | %-13s | %-27s | %-10s | %-10s |\n",
                "Country Code", "Country Name", "Continent", "Region", "Population", "Capital");
        System.out.println("+-----------------+------------------+------------+-----------------------------+------------+-------------------+");

        for (Country country : countries) {
            System.out.printf("| %-15s | %-19s | %-13s | %-27s | %-10d | %-10s |\n",
                    country.getCountryCode(), country.getName(), country.getContinent(),
                    country.getRegion(), country.getPopulation(), country.getCapital());
        }

        System.out.println("+-----------------+------------------+------------+-----------------------------+------------+-------------------+");
    }
}
