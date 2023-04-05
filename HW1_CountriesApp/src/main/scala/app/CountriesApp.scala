package app

import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._

import scala.util.{Try, Using, Failure, Success}
import scala.io.Source
import java.nio.file.{Files, Paths}


object CountriesApp extends App {
  case class Arguments(inputJsonPath: String = "",
                       outputJsonPath: String = "",
                       countryName: String = "",
                       countryCount: Int = 10)

  val parser = new scopt.OptionParser[Arguments]("Parsing application") {
    opt[String]('i', "inputJsonPath")
      .required().valueName("").action((value, arguments) => arguments.copy(inputJsonPath = value))

    opt[String]('o', "outputJsonPath")
      .required().valueName("").action((value, arguments) => arguments.copy(outputJsonPath = value))

    opt[String]('n', "countryName")
      .required().valueName("").action((value, arguments) => arguments.copy(countryName = value))

    opt[Int]('c', "countryCount")
      .required().valueName("").action((value, arguments) => arguments.copy(countryCount = value))
  }

  case class Name(common: String)

  case class Country(name: Name,
                     capital: List[String],
                     area: Double,
                     region: Option[String])

  case class CountryDto(name: String,
                        capital: String,
                        area: Double)

  object CountryDto {
    def createDto(country: Country): CountryDto =
      CountryDto(name = country.name.common,
        capital = country.capital.head,
        area = country.area)
  }


  def getJsonStringData(filePath: String): Try[List[Country]] = {
    implicit val inputCodec: JsonValueCodec[List[Country]] = JsonCodecMaker.make

    Using(Source.fromFile(filePath)) {
      source => readFromString[List[Country]](source.mkString)
    }
  }

  def writeJsonData(filePath: String, data: List[CountryDto]): Unit = {
    implicit val outputCodec: JsonValueCodec[List[CountryDto]] = JsonCodecMaker.make

    val result = writeToArray(data)

    Try(Files.write(Paths.get(filePath), result)) match {
      case Success(value) => println("Json was saved successfully")
      case Failure(exception) => println(s"Something went wrong while writing json: ${exception.printStackTrace()}")
    }
  }

  def getCountries(args: Arguments): Unit = {
    println(s"Input JSON path: ${args.inputJsonPath}")
    println(s"Output JSON path: ${args.outputJsonPath}")
    println(s"Country name: ${args.countryName}")
    println(s"Country count: ${args.countryCount}")

    val jsonStringData = getJsonStringData(args.inputJsonPath)
    //    jsonStringData.foreach(x => println(x))

    jsonStringData match {
      case Success(countries) => {
        val countriesDto: List[CountryDto] = countries
          .filter(x => x.region.contains(args.countryName))
          .sortBy(x => -x.area)
          .take(args.countryCount)
          .map(x => CountryDto.createDto(x))
        println(s"Got countries: $countriesDto")
        writeJsonData(args.outputJsonPath, countriesDto)
      }
      case Failure(exception) => println(s"Something went wrong: ${exception.printStackTrace()}")
    }

  }

  parser.parse(args, Arguments()) match {
    case Some(arguments) => getCountries(arguments)
    case None =>
  }

}
