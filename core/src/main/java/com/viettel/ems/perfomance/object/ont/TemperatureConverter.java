package com.viettel.ems.perfomance.object.ont;

public class TemperatureConverter {
   private double degreesKelvin;

   private TemperatureConverter(double degrees) {
       this.degreesKelvin = degrees;
   }

   public static TemperatureConverter fromKelvin(double degrees) {
       return new TemperatureConverter(degrees);
   }

   public static TemperatureConverter fromRankine(double degrees) {
       return new TemperatureConverter(5.0 * degrees / 9.0);
   }

   public static TemperatureConverter fromDelisle(double degrees) {
       return new TemperatureConverter(2.0 * (373.15 - degrees) / 3.0);
   }

    public static TemperatureConverter fromNewton(double degrees) {
       return new TemperatureConverter(100.0 * degrees / 33.0 + 273.15);
   }

    public static TemperatureConverter fromReaumur(double degrees) {
       return new TemperatureConverter(5.0  * degrees / 4.0 + 273.15);
   }

    public static TemperatureConverter fromRomer(double degrees) {
       return new TemperatureConverter(40 * (degrees - 7.5) / 21 + 273.15);
   }

    public static TemperatureConverter fromFahrenheit(double degrees) {
       return new TemperatureConverter(5 * (degrees - 459.67) / 9 );
   }

   public double toCelsius() {
    return this.degreesKelvin - 273.15;
   }


   public double customConvert(double multi, double plusFirst, double plus) {
//    return plusFirst ? (this.degreesKelvin +  plus) * multi : this.degreesKelvin * multi + plus;
       return 1.0;
   }
}