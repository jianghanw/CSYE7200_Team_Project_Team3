package controllers

import javax.inject._
import play.api._
import play.api.mvc._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(val controllerComponents: ControllerComponents) extends BaseController {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index())
  }

  def getList(idLst: String) = Action {implicit request:Request[AnyContent] =>

    val teams:Array[Int]=idLst.split(",").map(_.toInt)
    val team_Radiant=Array.ofDim[Int](5)
    val team_Dire=Array.ofDim[Int](5)
    for(i<-0 to 4) {
        team_Radiant(i) = teams(i)
        team_Dire(i) = teams(i + 5)
    }
    Ok(Predict.predict(team_Radiant,team_Dire))

  }

}
