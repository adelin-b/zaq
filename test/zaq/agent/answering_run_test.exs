defmodule Zaq.Agent.AnsweringRunTest do
  use ExUnit.Case, async: true

  alias Zaq.Agent.AnsweringRun

  test "strips inline source markers from an answer" do
    assert AnsweringRun.clean_answer("Final answer [[source:documents/report.pdf|p2]].") ==
             "Final answer."
  end

  test "normalizes whitespace left by inline source markers" do
    answer =
      "First claim [[source:documents/a.pdf|p1]].  \nSecond claim [[source:documents/a.pdf|p2]]."

    assert AnsweringRun.clean_answer(answer) ==
             """
             First claim.
             Second claim.
             """
             |> String.trim()
  end

  test "coerces nil and non-binary answers" do
    assert AnsweringRun.clean_answer(nil) == ""
    assert AnsweringRun.clean_answer(:atom_answer) == "atom_answer"
  end
end
