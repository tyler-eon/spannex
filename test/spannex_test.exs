defmodule SpannexTest do
  use ExUnit.Case
  doctest Spannex

  test "can insert data into tables" do
    assert {:ok, _} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               {:ok, _, [%{"id" => _}]} =
                 Spannex.execute(
                   conn,
                   "INSERT INTO foo (str, num) VALUES ('test1', 1) THEN RETURN id",
                   %{}
                 )
             end)
  end

  test "can read data without a formal transaction" do
    assert {:ok, _} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "INSERT INTO foo (str, num) VALUES ('test2', 2)",
                 %{}
               )
             end)

    assert {:ok, _, rows} = Spannex.execute(Spannex.Conn, "SELECT * FROM foo", %{})
    assert length(rows) > 0
  end

  test "can read data within a formal transaction" do
    assert {:ok, _} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "INSERT INTO foo (str, num) VALUES ('test3', 3)",
                 %{}
               )
             end)

    assert {:ok, {:ok, _, rows}} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "SELECT * FROM foo",
                 %{}
               )
             end)

    assert length(rows) > 0
  end

  test "supports complex queries" do
    assert {:ok, _} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "INSERT INTO foo (str, num) VALUES ('test4.1', 4), ('test4.2', 4), ('test4.3', 4)",
                 %{}
               )
             end)

    assert {:ok,
            {:ok, _,
             [
               %{"str" => "test4.1", "num" => 4, "flo" => nil},
               %{"str" => "test4.2", "num" => 4, "flo" => nil},
               %{"str" => "test4.3", "num" => 4, "flo" => nil}
             ]}} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "SELECT * FROM foo WHERE num > 3 and num < 5 ORDER BY str ASC",
                 %{}
               )
             end)
  end

  test "supports named variables" do
    assert {:ok, {:ok, _, _}} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "INSERT INTO foo (str, num) VALUES (@str, @num)",
                 %{str: "test5", num: 5}
               )
             end)

    assert {:ok, _,
            [
              %{"str" => "test5", "num" => 5, "flo" => nil}
            ]} =
             Spannex.execute(
               Spannex.Conn,
               "SELECT * FROM foo WHERE str = @str",
               %{str: "test5"}
             )
  end

  test "supports both integers (int64) and other numeric types (e.g. float)" do
    assert {:ok, _} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "INSERT INTO foo (str, num, flo) VALUES ('test6.1', 6, 6.1), ('test6.2', 6, 6.2)",
                 %{}
               )
             end)

    assert {:ok,
            {:ok, _,
             [
               %{"str" => "test6.1", "num" => 6, "flo" => 6.1},
               %{"str" => "test6.2", "num" => 6, "flo" => 6.2}
             ]}} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               Spannex.execute(
                 conn,
                 "SELECT * FROM foo WHERE num = @num and flo > @flo_min and flo < @flo_max ORDER BY str ASC",
                 %{num: 6, flo_min: 6.0, flo_max: 6.3}
               )
             end)
  end

  test "supports multiple executions per transaction" do
    assert {:ok, _} =
             Spannex.transaction(Spannex.Conn, fn conn ->
               assert {:ok, _, [%{"id" => foo_id}]} =
                        Spannex.execute(
                          conn,
                          "INSERT INTO foo (str, num) VALUES ('test7.1', 7) THEN RETURN id",
                          %{}
                        )

               assert {:ok, _,
                       [
                         %{
                           "foo_id" => ^foo_id,
                           "notnullstr" => "test7.2",
                           "notnullnum" => 7
                         }
                       ]} =
                        Spannex.execute(
                          conn,
                          "INSERT INTO bar (foo_id, notnullstr, notnullnum) VALUES (@foo_id, 'test7.2', 7) THEN RETURN foo_id, notnullstr, notnullnum",
                          %{foo_id: foo_id}
                        )
             end)
  end
end
