import scala.collection.mutable._
object helloWorldBigData {
  //premier programme Scala
  val ma_var_imm: String = "Formation Big Data"
  private val une_var_imm = "Formation Big Data"

  class Person(var nom : String, var prenom: String, var age: Int)

  def main(args: Array[String]): Unit = {
    println("Hello world: mon premier programme scala")

    val test_mu1: Int = 15 // variable immutable
    var test_mu: Int = 15 // variable mutable
    test_mu = test_mu + 10
    println(test_mu)

    println("Votre texte contient: "+ Comptage_caracteres("Qu'avez vous mangé ce matin ."))

    getResultat(10)
    test_while(10)
    test_For()
    collectionScala()
    collectionTuple()



  }
  //Ma première fonction
  def Comptage_caracteres(texte: String): Int = {
    if (texte.isEmpty){
      println("Vous n'avez renseigné aucun texte.")
    }
    texte.trim.length()

  }

  //Deuxieme façon d'écrire une fonction
  def Comptage_caracteres2(texte: String): Int = {
    return texte.trim.length()

  }
  //Troisieme façon d'écrire une fonction
  def Comptage_caracteres3(texte: String): Int = texte.trim.length()

  //ma première méthode/procédure
  def getResultat(parametre: Any) : Unit ={
    if (parametre==10){
      println("Votre valeur est un entier.")
    }
    else {
      println("Votre valeur n'est pas un entier.")

    }

  }

  //structures conditionnelles
  def test_while(valeur_cond:Int): Unit ={
    var i: Int =0
    while (i<valeur_cond){
      println("Iteration while N° "+i)
      i=i+1
    }
  }
  def test_For(): Unit ={
    var i: Int =0
    for (i<- 5 to 15 by(2)){
      println("Iteration for N° "+i)
    }
  }
  //Les collections en scala
  def collectionScala(): Unit = {
    val ma_liste : List[Int]= List(1,2,3, 10, 45, 15)
    val ma_liste_S : List[String] = List("joel", "ed", "chris", "maurice", "julien")
    val chaine: String ="Ma chaine"

    val plage_v: List[Int]= List.range(1,15,2)
    println(ma_liste(0))
    for (i<-ma_liste_S){
      println(i)
    }
    //Manipulation des collections à l'aide des fonctions anonymes
    val results: List[String] = ma_liste_S.filter(e => e.endsWith("n"))
    for(r<- results){
      println(r)
    }
    val res: Int= ma_liste_S.count(i=> i.endsWith("n"))
    println("Nombre d'éléments "+res)

    val ma_liste2: List[Int] = ma_liste.map(_* 2)
    for(r<- ma_liste2){
      println(r)
    }

    val ma_liste3: List[Int] = ma_liste.map(e => e * 2)
    val ma_liste4: List[Int] = ma_liste.map(_ * 2)

    val nouvelle_liste: List[Int]= plage_v.filter(p =>p >5)
    val  new_liste : List[String]=ma_liste_S.map(s => s.capitalize)
    new_liste.foreach(e => println("Nouvelle liste: "+e))
    nouvelle_liste.foreach(e => println("Nouvelle liste: "+e))
    plage_v.foreach(println(_))


  }

  def collectionTuple() : Unit= {
    val tuple_test = ("45","JVC", "FALSE")
    println(tuple_test._3)

   val Nouvelle_personne: Person = new Person("Choukogoue", "Juvenal", 32)
    val tuple_2 =("test", "nouvelle personne", 45)
    tuple_2.toString().toList


    //table de hachage
    val state= Map(
      "AK" ->"Alaska",
      "IL"->"Illinois",
      "KY"->"Kentucky"
    )

    val  personne = Map(
      "nom" ->"CHOUKOGOUE",
      "prenom"->"JUVENAL",
      "age"->45
    )

    //les tableaux
    val mon_tableau: Array[String]=Array("Juv", "JVC", "test")
    mon_tableau.foreach(e => println(e))


  }





}
