package edu.neu.csye7200

import DataProcess._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataSpec  extends AnyFlatSpec with Matchers{
  behavior of "raw dataset"
  it should "work for raw data" in {
    val rowPlayers = players.count()
    val rowMatch = matches.count()
    val synergy_row=synergy_result.count()
    val countering_row=countering_result.count()
    val row_tm=test_labels.count()
    val row_tp=test_player.count()
    rowPlayers shouldBe 500000
    rowMatch shouldBe 500000
    synergy_row shouldBe 113
    countering_row shouldBe 113
    row_tm shouldBe 1000000
    row_tp shouldBe 1000000
  }

  behavior of "data strict"
  it should "work for processed" in {
    val numRW = players.filter(players("radiant_win") === "true").count()
    val numDW = players.filter(players("radiant_win") === "flase").count()
    numRW+numDW shouldBe 50000

    val heroName114 = heronames.filter(heronames("hero_id") >= 114).count()
    heroName114 shouldBe 0
    val heroName0 = heronames.filter(heronames("hero_id") < 0).count()
    heroName0 shouldBe 0
    val heroName24 = heronames.filter(heronames("hero_id") === 24).count()
    heroName0 shouldBe 0
    val heroName108 = heronames.filter(heronames("hero_id") === 108).count()
    heroName0 shouldBe 0
    val heroName113 = heronames.filter(heronames("hero_id") === 113).count()
    heroName0 shouldBe 0

    val player_slot0 = players.filter(players("player_slot") < 0).count()
    player_slot0 shouldBe 0
    val player_slot4128 = players.filter(players("player_slot") > 4 && players("player_slot") < 128).count()
    player_slot4128 shouldBe 0
    val player_slot132 = players.filter(players("player_slot") > 132 ).count()
    player_slot132 shouldBe 0

    val player_hero0= players.filter(players("hero_id") < 0 ).count()
    player_hero0 shouldBe 0
    val player_hero24= players.filter(players("hero_id") === 24 ).count()
    player_hero24 shouldBe 0
    val player_hero113= players.filter(players("hero_id") === 113 ).count()
    player_hero113 shouldBe 0
    val player_hero108= players.filter(players("hero_id") === 108 ).count()
    player_hero108 shouldBe 0
  }
}
