port module Main exposing (..)

import Browser
import Html exposing (Attribute, input)
import Html.Attributes exposing (class, placeholder, property, type_)
import Html.Events exposing (onInput)
import Json.Encode as Encode



{-
   Boilerplate code
-}


dataTest : String -> Attribute msg
dataTest name =
    property "data-test" (Encode.string name)


port searchInputChange : String -> Cmd msg


main : Program () {} Msg
main =
    Browser.element
        { init = \_ -> ( {}, Cmd.none )
        , view = view
        , update = update
        , subscriptions = \_ -> Sub.none
        }


type Msg
    = InputChange String


update msg model =
    case Debug.log "[elm]" msg of
        InputChange newInput ->
            ( {}, searchInputChange newInput )


view model =
    input [ type_ "text", class "form-control", placeholder "search table/view/function", dataTest "search-tables", onInput InputChange ] []
