package org.apache.spark.iml

import com.microsoft.azure.synapse.ml.io.IOImplicits._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.scalatest.Ignore
import org.scalatest.flatspec._
import org.scalatest.matchers._

/**
 * Examples for model serving with SynapseML
 */
@Ignore
class ServingModelTest extends AnyFlatSpec with should.Matchers with WithSpark with WithDatasets {

  /**
   * Start REST server with logreg model
   */
  lazy val (server, url) = {
    val schema = "client_id STRING,gender INT,Veterinarnyye_uslugi DOUBLE,General_nyye_podryadchiki_po_ventilyatsii_teplosnabzheniyu_i_vodoprovodu DOUBLE,Podryadchiki_po_elektrichestvu DOUBLE,Podryadchiki_spetsializirovannaya_torgovlya_nigde_boleye_ne_klassifitsirovannyye DOUBLE,Raznoobraznyye_izdatel_stva_pechatnoye_delo DOUBLE,Avialinii_aviakompanii DOUBLE,Agent_stva_po_arende_avtomobiley DOUBLE,Zhil_ye_oteli_moteli_kurorty DOUBLE,Transportirovka_prigorodnyye_i_lokal_nyye_sezonnyye_transportnyye_sredstva_vklyuchaya_elektrichki DOUBLE,Passazhirskiye_zheleznyye_perevozki DOUBLE,Limuziny_i_taksi DOUBLE,Avtobusnyye_linii DOUBLE,Agent_stva_po_avtotransportnym_perevozkam_mestnyye_dal_nyye_avtogruzoperevozki_kompanii_popereyezdu_i_khraneniyu_mestnaya_dostavka DOUBLE,Uslugi_kur_yera_po_vozdukhu_i_na_zemle_agent_stvo_po_otpravke_gruzov DOUBLE,Kruiznyye_linii DOUBLE,Avialinii_aviakompanii_nigde_boleye_ne_klassifitsirovannyye DOUBLE,Turisticheskiye_agent_stva_i_organizatory_ekskursiy DOUBLE,Dorozhnyy_i_mostovoy_sbory_poshliny DOUBLE,Uslugi_po_transportirovke_nigde_boleye_ne_klassifitsirovannyye DOUBLE,Telekommunikatsionnoye_oborudovaniye_vklyuchaya_prodazhu_telefonov DOUBLE,Zvonki_s_ispol_zovaniyem_telefonov_schityvayushchikh_magnitnuyu_lentu DOUBLE,Komp_yuternaya_set_informatsionnyye_uslugi DOUBLE,Denezhnyye_perevody DOUBLE,Kabel_nyye_i_drugiye_platnyye_televizionnyye_uslugi DOUBLE,Kommunal_nyye_uslugi_elektrichestvo_gaz_sanitariya_voda DOUBLE,Postavshchiki_gruzovikov_i_zapchastey DOUBLE,Stroitel_nyye_materialy_nigde_boleye_ne_klassifitsirovannyye DOUBLE,Ofisnoye_fotograficheskoye_fotokopiroval_noye_i_mikrofil_miruyushcheye_oborudovaniye DOUBLE,Komp_yutery_periferiynoye_komp_yuternoye_oborudovaniye_programmnoye_obespecheniye DOUBLE,Stomatologicheskoye_laborotornoye_meditsinskoye_oftal_mologicheskoye_statsionarnoye_oborudovaniye_i_ustroystva DOUBLE,Elektricheskiye_chasti_i_oborudovaniye DOUBLE,Oborudovaniye_i_soput_stvuyushchiye_materialy_dlya_tekhnicheskogo_obespecheniya DOUBLE,Oborudovaniye_dlya_vodoprovoda_i_otopitel_noy_sistemy DOUBLE,Promyshlennoye_oborudovaniye_nigde_boleye_ne_klassifitsirovannoye DOUBLE,Dragotsennyye_kamni_i_metally_chasy_i_yuvelirnyye_izdeliya DOUBLE,Tovary_dlitel_nogo_pol_zovaniya_nigde_boleye_ne_klassifitsirovannyye DOUBLE,Kantselyariya_ofisnyye_soput_stvuyushchiye_tovary_bumaga_dlya_pechataniya_i_pis_ma DOUBLE,Lekarstva_ikh_rasprostraniteli_apteki DOUBLE,Shtuchnyye_tovary_galantereya_i_drugiye_tekstil_nyye_tovary DOUBLE,Muzhskaya_zhenskaya_i_det_skaya_spets_odezhda DOUBLE,Khimikalii_i_smezhnyye_veshchestva_ne_klassifitsirovannyye_raneye DOUBLE,Neft_i_nefteprodukty DOUBLE,Knigi_periodicheskiye_izdaniya_i_gazety DOUBLE,Oborudovaniye_dlya_vyrashchivaniya_rasteniy_inventar_dlya_pitomnikov_i_tsvety DOUBLE,Tovary_nedlitel_nogo_pol_zovaniya_ne_klassifitsirovannyye_raneye DOUBLE,Tovary_dlya_doma DOUBLE,Leso_i_stroitel_nyy_material DOUBLE,Roznichnaya_prodazha_stekla_krasok_i_oboyev DOUBLE,Skobyanyye_tovary_v_roznitsu DOUBLE,Sadovyye_prinadlezhnosti_v_tom_chisle_dlya_ukhoda_za_gazonami_v_roznitsu DOUBLE,Optoviki DOUBLE,Besposhlinnyye_magaziny_Duty_Free DOUBLE,Magaziny_torguyushchiye_po_snizhennym_tsenam DOUBLE,Univermagi DOUBLE,Universal_nyye_magaziny DOUBLE,Razlichnyye_tovary_obshchego_naznacheniya DOUBLE,Bakaleynyye_magaziny_supermarkety DOUBLE,Prodazha_svezhego_i_morozhenogo_myasa DOUBLE,Konditerskiye DOUBLE,Prodazha_molochnykh_produktov_v_roznitsu DOUBLE,Bulochnyye DOUBLE,Razlichnyye_prodovol_stvennyye_magaziny_rynki_magaziny_so_spets_assortimentom_prodazha_polufabrikatov_firmennykh_blyud_prodazha_s_pomoshch_yu_torgovykh_avtomatov DOUBLE,Legkovoy_i_gruzovoy_transport_prodazha_servis_remont_zapchasti_i_lizing DOUBLE,Avtoshiny DOUBLE,Avtozapchasti_i_aksessuary DOUBLE,Stantsii_tekhobsluzhivaniya DOUBLE,Avtomatizirovannyye_benzozapravki DOUBLE,Prodazha_mototsiklov DOUBLE,Prodazha_snegokhodov DOUBLE,Muzhskaya_odezhda_i_aksessuary_vklyuchaya_odezhdu_dlya_mal_chikov DOUBLE,Gotovaya_zhenskaya_odezhda DOUBLE,Aksessuary_dlya_zhenshchin DOUBLE,Det_skaya_odezhda_vklyuchaya_odezhdu_dlya_samykh_malen_kikh DOUBLE,Odezhda_dlya_vsey_sem_i DOUBLE,Sportivnaya_odezhda_odezhda_dlya_verkhovoy_yezdy_i_yezdy_na_mototsikle DOUBLE,Obuvnyye_magaziny DOUBLE,Izgotovleniye_i_prodazha_mekhovykh_izdeliy DOUBLE,Magaziny_muzhskoy_i_zhenskoy_odezhdy DOUBLE,Uslugi_po_peredelke_pochinke_i_poshivu_odezhdy DOUBLE,Razlichnyye_magaziny_odezhdy_i_aksessuarov DOUBLE,Oborudovaniye_mebel_i_bytovyye_prinadlezhnosti_krome_elektrooborudovaniya_ DOUBLE,Pokrytiya_dlya_pola DOUBLE,Tkani_obivochnyy_material_gardiny_i_port_yery_zhalyuzi DOUBLE,Razlichnyye_spetsializirovannyye_magaziny_bytovykh_prinadlezhnostey DOUBLE,Bytovoye_oborudovaniye DOUBLE,Prodazha_elektronnogo_oborudovaniya DOUBLE,Prodazha_muzykal_nykh_instrumentov_fortepiano_not DOUBLE,Prodazha_komp_yuternogo_programmnogo_obespecheniya DOUBLE,Magaziny_zvukozapisi DOUBLE,Postavshchiki_provizii DOUBLE,Mesta_obshchestvennogo_pitaniya_restorany DOUBLE,Bary_kokteyl_bary_diskoteki_nochnyye_kluby_i_taverny_mesta_prodazhi_alkogol_nykh_napitkov DOUBLE,Restorany_zakusochnyye DOUBLE,Tsifrovyye_tovary_igry DOUBLE,Apteki DOUBLE,Magaziny_s_prodazhey_spirtnykh_napitkov_na_vynos_pivo_vino_i_liker_ DOUBLE,Magaziny_second_hand_magaziny_b_u_tovarov_komissionki DOUBLE,Velomagaziny_prodazha_i_obsluzhivaniye DOUBLE,Magaziny_sporttovarov DOUBLE,Knizhnyye_magaziny DOUBLE,Magaziny_ofisnykh_shkol_nykh_prinadlezhnostey_kantstovarov DOUBLE,Magaziny_po_prodazhe_chasov_yuvelirnykh_izdeliy_i_izdeliy_iz_serebra DOUBLE,Magaziny_igrushek DOUBLE,Magaziny_fotooborudovaniya_i_fotopriborov DOUBLE,Magaziny_otkrytok_podarkov_novinok_i_suvenirov DOUBLE,Magaziny_kozhanykh_izdeliy_dorozhnykh_prinadlezhnostey DOUBLE,Magaziny_tkani_nitok_rukodeliya_shit_ya DOUBLE,Magaziny_khrustalya_i_izdeliy_iz_stekla DOUBLE,Pryamoy_marketing_torgovlya_cherez_katalog DOUBLE,Pryamoy_marketing_kombinirovannyy_katalog_i_torgovtsy_v_roznitsu DOUBLE,Pryamoy_marketing_vkhodyashchiy_telemarketing DOUBLE,Pryamoy_marketing_torgovyye_tochki_podpiski DOUBLE,Pryamoy_marketing_drugiye_torgovyye_tochki_pryamogo_marketinga_nigde_boleye_ne_klassifitsirovannyye_ DOUBLE,Magaziny_khudozhestvennykh_i_remeslennykh_izdeliy DOUBLE,Galerei_i_khudozhestvennyye_posredniki DOUBLE,Ortopedicheskiye_tovary DOUBLE,Magaziny_kosmetiki DOUBLE,Goryucheye_toplivo_ugol_neft_razzhizhennyy_benzin_drova DOUBLE,Floristika DOUBLE,Tabachnyye_magaziny DOUBLE,Dilery_po_prodazhe_pechatnoy_produktsii DOUBLE,Zoomagaziny DOUBLE,Plavatel_nyye_basseyny_rasprodazha DOUBLE,Finansovyye_instituty_snyatiye_nalichnosti_vruchnuyu DOUBLE,Finansovyye_instituty_snyatiye_nalichnosti_avtomaticheski DOUBLE,Finansovyye_instituty_torgovlya_i_uslugi DOUBLE,Ne_finansovyye_instituty_inostrannaya_valyuta_denezhnyye_perevody_neperedavayemyye_dorozhnyye_cheki_kvazi_k_esh DOUBLE,Tsennyye_bumagi_brokery_dilery DOUBLE,Prodazha_strakhovaniya_garantirovannoye_razmeshcheniye_premii DOUBLE,Agenty_i_menedzhery_po_arende_nedvizhimosti DOUBLE,Denezhnyye_perevody_MasterCard_MoneySend DOUBLE,Oteli_moteli_bazy_otdykha_servisy_bronirovaniya DOUBLE,Taymsher DOUBLE,Khimchistki DOUBLE,Fotostudii DOUBLE,Saloty_krasoty_i_parikmakherskiye DOUBLE,Sluzhby_znakomstv DOUBLE,Servisy_po_pokupke_prodazhe DOUBLE,Tsentry_zdorov_ya DOUBLE,Inoy_servis DOUBLE,Reklamnyye_uslugi DOUBLE,Uslugi_kopiroval_nykh_tsentrov DOUBLE,Programmirovaniye_obrabotka_dannykh_integrirovannyye_sistemy_dizayn DOUBLE,Informatsionnyye_provaydery DOUBLE,Fotostudii_fotolaboratorii DOUBLE,Biznes_servis DOUBLE,Prokat_avtomobiley DOUBLE,Parkingi_i_garazhi DOUBLE,Stantsii_tekhnicheskogo_obsluzhivaniya_dlya_avtomobil_nogo_transporta DOUBLE,STO_obshchego_naznacheniya DOUBLE,Avtomoyki DOUBLE,Remont_bytovoy_tekhniki_remont_elektropribory DOUBLE,Obshchiy_remont DOUBLE,Proizvodstvo_i_distributsiya_videofil_mov DOUBLE,Kinoteatry DOUBLE,Videoprokat DOUBLE,Teatral_nyye_prodyuserskiye_agent_stva DOUBLE,Bill_yard_kluby DOUBLE,Bouling_kluby DOUBLE,Turisticheskiye_attraktsiony_i_shou DOUBLE,Prinadlezhnosti_dlya_videoigr DOUBLE,Galerei_uchrezhdeniya_videoigr DOUBLE,Tranzaktsii_po_azartnym_igram DOUBLE,Luna_parki_karnavaly_tsirki_predskazateli_budushchego DOUBLE,Kluby_sel_skiye_kluby_chlenstvo_sportivnyy_otdykh_sport_chastnyye_polya_dlya_gol_fa DOUBLE,Uslugi_otdykha_nigde_raneye_ne_klassifitsiruyemyye DOUBLE,Doktora_nigde_raneye_ne_klassifitsiruyemyye DOUBLE,Dantisty_ortodontisty DOUBLE,Optika_opticheskiye_tovary_i_ochki DOUBLE,Bol_nitsy DOUBLE,Zubnyye_i_meditsinskiye_laboratorii DOUBLE,Praktikuyushchiye_vrachi_meditsinskiye_uslugi_nigde_raneye_ne_klassifitsiruyemyye DOUBLE,Kolledzhi_universitety_professional_nyye_shkoly_i_mladshiye_kolledzhi DOUBLE,Shkoly_biznes_i_sekretarey DOUBLE,Obrazovatel_nyye_uslugi DOUBLE,Organizatsii_blagotvoritel_nyye_i_obshchestvennyye_sluzhby DOUBLE,Assotsiatsii_grazhdanskiye_sotsial_nyye_i_brat_skiye DOUBLE,Organizatsii_chlenstva_nigde_raneye_ne_klassifitsiruyemyye DOUBLE,Professional_nyye_uslugi_nigde_raneye_ne_klassifitsiruyemyye DOUBLE,Sudovyye_vyplaty_vklyuchaya_alimenty_i_det_skuyu_podderzhku DOUBLE,Shtrafy DOUBLE,Nalogovyye_platezhi DOUBLE,Pravitel_stvennyye_uslugi_nigde_raneye_ne_klassifitsiruyemyye DOUBLE,Pochtovyye_uslugi_tol_ko_pravitel_stvo DOUBLE"

    val listener: DataFrame = spark
      .readStream
      .server
      .address("localhost", 8898, "my_api")
      .load()
      .parseRequest("my_api", StructType.fromDDL(schema))

    val model = PipelineModel.load("./models/logReg")
    val outputs = model.transform(listener).makeReply("prediction")

    val server = outputs.writeStream
      .server
      // Note that replyTo has a different semantics in normal and continous server.
      // For ordinary server it must match name of api (third parameter of address)
      // But for continous server it must match name of the server (name option on the listener)
      .replyTo("my_api")
      .queryName("my_query")
      .option("checkpointLocation", "./checkpoints/" + Identifiable.randomUID("logReg"))
      .start()

    (server, "http://localhost:8898/my_api")
  }

  "Server" should "start web service" in {
    val result = requests.post(url, data = Map())
    result.statusCode should be(200)
  }

  /**
   * Make sure that a user with transaction primarily in cosmetics, beauty and fashion is
   * classified as female
   */
  "Server" should "spendings in cosmetics should be classified as female" in {
    val result = requests.post(
      url = "http://localhost:8898/my_api",
      data = """{
          |"Magaziny_kosmetiki": -100,
          |"Gotovaya_zhenskaya_odezhda": -200,
          |"Obuvnyye_magaziny":-300,
          |"Apteki":-400,
          |"Aksessuary_dlya_zhenshchin":-500,
          |"Bol_nitsy": -600,
          |"Magaziny_tkani_nitok_rukodeliya_shit_ya": -700,
          |"Magaziny_ofisnykh_shkol_nykh_prinadlezhnostey_kantstovarov": -800,
          |"Knizhnyye_magaziny": -900,
          |"Saloty_krasoty_i_parikmakherskiye": -1000}""".stripMargin
    )

    result.data.toString() should be("""{"prediction":0.0}""")
  }

  /**
   * Make sure that a user with spendings in auto and electronics is classified as male
   */
  "Server" should "spendings in autostores should be classified as male" in {
    val result = requests.post(
      url = url,
      data = """{
               |"Stantsii_tekhobsluzhivaniya": -100,
               |"Galerei_uchrezhdeniya_videoigr": -200,
               |"Goryucheye_toplivo_ugol_neft_razzhizhennyy_benzin_drova":-300,
               |"Mesta_obshchestvennogo_pitaniya_restorany":-400,
               |"Muzhskaya_odezhda_i_aksessuary_vklyuchaya_odezhdu_dlya_mal_chikov":-500,
               |"Zhil_ye_oteli_moteli_kurorty": -600,
               |"Prodazha_elektronnogo_oborudovaniya": -700,
               |"STO_obshchego_naznacheniya": -800,
               |"Postavshchiki_gruzovikov_i_zapchastey": -900,
               |"total": -1000}""".stripMargin
    )

    result.data.toString() should be("""{"prediction":1.0}""")
  }

}