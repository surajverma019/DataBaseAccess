using System;
using System.Data.SqlClient;
using System.Data;

public class DataBaseAccess
{
    public DataBaseAccess()
    {
        //   0 : Correct Result
        //  -1 : Connection Error
        //  -2 : Insert  Error
        //  -3 : Delete  Error
        //  -4 : Update  Error 
        //  -5 : insert delete update error in store procedure method 
    }

    string connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString.ToString();
    SqlConnection connection = new SqlConnection();
    SqlCommand command = new SqlCommand();
    SqlDataAdapter adapter = new SqlDataAdapter();
    DataSet ds = new DataSet();
    int result = 0;
    string query = null;
    public string error { get; set; }


    public int open_connection()
    {
        try
        {
            connection = new SqlConnection(connectionString);
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            command.Connection = connection;
        }
        catch (Exception ex) { result = -1; error = ex.Message + "||" + ex.StackTrace; }
        return result;
    }
    public void close_connection()
    {
        if (connection.State == ConnectionState.Open)
        {
            connection.Close();
            connection.Dispose();
            command.Dispose();
        }
    }
    public int Insert(string str)
    {
        int i = 0;
        try
        {
            open_connection();
        }
        catch (Exception ex)
        {
            i = -1; error = ex.Message + "||" + ex.StackTrace;
        }
        try
        {
            command.CommandType = CommandType.Text;
            command.CommandText = str;
            command.ExecuteNonQuery();
        }
        catch (Exception ex) { i = -2; error = ex.Message + "||" + ex.StackTrace; }
        close_connection();

        return i;
    }

    public int insert_into_table(string table_name, string column_names_with_comma, string column_values_with_comma)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                query = "insert into " + table_name + "(" + column_names_with_comma + ") values (" + column_values_with_comma + ")  ";
                command = new SqlCommand(query);
                command.ExecuteNonQuery();
            }
            catch (Exception ex) { result = -2; error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return result;
    }
    public int delete_from_table(string table_name, string condition_columns_values)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                query = "delete from " + table_name + " where " + condition_columns_values + "  ";
                command = new SqlCommand(query);
                command.ExecuteNonQuery();

            }
            catch (Exception ex) { result = -3; error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return result;
    }
    public int update_table(string table_name, string set_column_names_values, string condition_columns_values)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                query = "update " + table_name + " set " + set_column_names_values + " where  " + condition_columns_values + "  ";
                command = new SqlCommand(query);
                command.ExecuteNonQuery();

            }
            catch (Exception ex) { result = -4; error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return result;
    }
    public int insert_delete_update_by_storeprocedure_(string storeprocedure_name, string[] parameter_names, string[] parameter_values)
    {
        try
        {
            open_connection();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = storeprocedure_name;
            for (int i = 0; i < parameter_names.Length && i < parameter_values.Length; i++)
            {
                SqlParameter sparam = new SqlParameter(parameter_names[i], parameter_values[i]);
                command.Parameters.Add(sparam);
            }
            command.Parameters.Add("@id", SqlDbType.Int).Direction = ParameterDirection.Output;
            command.ExecuteNonQuery();
            result = Convert.ToInt32(command.Parameters["@id"].Value);
        }
        catch (Exception ex)
        { result = -5; error = ex.Message + "||" + ex.StackTrace; }
        close_connection();
        return result;
    }

    public int insert_delete_update_by_storeprocedure(string storeprocedure_name, string[] parameter_names, string[] parameter_values)
    {
        using (SqlConnection sqlcon = new SqlConnection(connectionString))
        {
            using (SqlCommand cmd = new SqlCommand(storeprocedure_name, sqlcon))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                for (int i = 0; i < parameter_names.Length && i < parameter_values.Length; i++)
                {
                    SqlParameter sparam = new SqlParameter(parameter_names[i], parameter_values[i]);
                    cmd.Parameters.Add(sparam);
                }
                cmd.Parameters.Add("@id", SqlDbType.Int).Direction = ParameterDirection.Output;
                try
                {
                    sqlcon.Open();
                    cmd.ExecuteNonQuery();
                    result = Convert.ToInt32(cmd.Parameters["@id"].Value);
                    sqlcon.Close();
                }
                catch (Exception ex)
                {
                    result = -5;
                    error = ex.Message + "||" + ex.StackTrace;
                }
                finally
                {
                    sqlcon.Close();
                    cmd.Dispose();
                    sqlcon.Dispose();
                }
            }
        }
        return result;
    }

    //public int AddItemsToDatabase(AttachmentsList items)
    //{
    //    try
    //    {
    //        using (SqlConnection sqlcon = new SqlConnection(connectionString))
    //        {
    //            using (SqlCommand cmd = new SqlCommand("AddAttachment", sqlcon))
    //            {
    //                // add the table-valued-parameter. 
    //                cmd.CommandType = CommandType.StoredProcedure;
    //                cmd.Parameters.Add("@Items", SqlDbType.Structured).Value = items.GetItemsAsDataTable();
    //                // execute
    //                sqlcon.Open();
    //                cmd.ExecuteNonQuery();
    //                sqlcon.Close();
    //                result = 0;
    //            }
    //        }
    //    }
    //    catch (Exception ex)
    //    {
    //        error = ex.Message + " | " + ex.StackTrace;
    //        result = -5;
    //    }
    //    return result;
    //}

    public int insert_delete_update_by_storeprocedure1(string storeprocedure_name, string[] parameter_names, string[] parameter_values)
    {
        int res = 0;
        try
        {
            open_connection();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = storeprocedure_name;
            for (int i = 0; i < parameter_names.Length && i < parameter_values.Length; i++)
            {
                SqlParameter sparam = new SqlParameter(parameter_names[i], parameter_values[i]);
                command.Parameters.Add(sparam);
            }
            command.ExecuteNonQuery();
            res = 0;
        }
        catch (Exception ex)
        {
            res = 0;
            error = ex.Message + "|" + ex.StackTrace;
        }
        close_connection();
        return res;

    }

    public DataSet get_dataset_by_parameters(string select_column_names, string table_name, string condition_columns_values)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                query = "select  " + select_column_names + " from " + table_name + " where  " + condition_columns_values + "  ";
                adapter = new SqlDataAdapter(query, connection);
                adapter.Fill(ds);

            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return ds;
    }
    public DataSet get_dataset_by_query(string query)
    {
        DataSet ds = new DataSet();
        result = open_connection();
        if (result != -1)
        {
            try
            {
                adapter = new SqlDataAdapter(query, connection);
                adapter.Fill(ds);

            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return ds;
    }
    public DataTable get_dataTable_by_query(string query)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                adapter = new SqlDataAdapter(query, connection);
                adapter.Fill(ds);

            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return ds.Tables[0];
    }
    //Example 
    //string[] parameter_names = { "@product_name","@description","@status" };All parameter with column names
    //string[] parameter_values = { prod_name,  descr , "1" }; All are column values objects     
    public DataSet get_dataset_by_storeprocedure(string storeprocedure_name, string[] parameter_names, string[] parameter_values)
    {
        try
        {
            open_connection();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = storeprocedure_name;

            for (int i = 0; i < parameter_names.Length && i < parameter_values.Length; i++)
            {
                SqlParameter sparam = new SqlParameter(parameter_names[i], parameter_values[i]);
                command.Parameters.Add(sparam);
            }
            adapter.SelectCommand = command;
            adapter.Fill(ds);

        }
        catch (Exception ex)
        { error = ex.Message + "||" + ex.StackTrace; }
        close_connection();
        return ds;
    }
    public DataTable get_dataTable_by_storeprocedure(string storeprocedure_name, string[] parameter_names, string[] parameter_values)
    {
        try
        {
            open_connection();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = storeprocedure_name;

            for (int i = 0; i < parameter_names.Length && i < parameter_values.Length; i++)
            {
                SqlParameter sparam = new SqlParameter(parameter_names[i], parameter_values[i]);
                command.Parameters.Add(sparam);
            }
            adapter.SelectCommand = command;
            adapter.Fill(ds);

        }
        catch (Exception ex)
        { error = ex.Message + "||" + ex.StackTrace; }
        close_connection();
        return ds.Tables[0];
    }

    public int Exist(string str)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                command.CommandType = CommandType.Text;
                command.CommandText = str;
                result = (int)command.ExecuteScalar();
            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return result;
    }

    public SqlDataReader getdata(string str)
    {
        result = open_connection();
        SqlDataReader dr = null;
        if (result != -1)
        {
            try
            {

                command.CommandType = CommandType.Text;
                command.CommandText = str;
                dr = command.ExecuteReader();
            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return dr;
    }
    public int get_max_scalar(string tbname, string maxcal)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                String str = "select max(" + maxcal + ") from " + tbname;
                command.CommandType = CommandType.Text;
                command.CommandText = str;
                result = Convert.ToInt16(command.ExecuteScalar());
            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return result;
    }
    public int get_next_identity(string tablename, string colname)
    {
        result = open_connection();
        if (result != -1)
        {
            try
            {
                command = new SqlCommand("select max(" + colname + ") as NextIdent from " + tablename);
                command.CommandType = CommandType.Text;
                adapter = new SqlDataAdapter(command);
                adapter.Fill(ds);
                if (ds.Tables[0].Rows[0]["NextIdent"].ToString() != "")
                    return Convert.ToInt32(ds.Tables[0].Rows[0]["NextIdent"].ToString()) + 1;
                else return 1;
            }
            catch (Exception ex) { error = ex.Message + "||" + ex.StackTrace; }
        }
        close_connection();
        return result;
    }

    public DataSet get_dataset_by_param_query(string query, string[] parameternames, string[] parameterValues)
    {
        DataSet ds = new DataSet();
        SqlCommand cmd = new SqlCommand(query);
        for (int i = 0; i < parameternames.Length; i++)
            cmd.Parameters.AddWithValue(parameternames[i], parameterValues[i]);
        String strConnString = System.Configuration.ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        SqlConnection con = new SqlConnection(strConnString);
        SqlDataAdapter sda = new SqlDataAdapter();
        cmd.CommandType = CommandType.Text;
        cmd.Connection = con;
        try
        {
            con.Open();
            sda.SelectCommand = cmd;
            sda.Fill(ds);
            return ds;
        }
        catch (Exception ex)
        {
            error = ex.Message + "||" + ex.StackTrace;
            return null;
        }
        finally
        {
            con.Close();
            sda.Dispose();
            con.Dispose();
        }
    }
    public DataTable get_dataTable_by_param_query(string query, string[] parameternames, string[] parameterValues)
    {
        DataTable ds = new DataTable();
        SqlCommand cmd = new SqlCommand(query);
        for (int i = 0; i < parameternames.Length; i++)
            cmd.Parameters.AddWithValue(parameternames[i], parameterValues[i]);
        String strConnString = System.Configuration.ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        SqlConnection con = new SqlConnection(strConnString);
        SqlDataAdapter sda = new SqlDataAdapter();
        cmd.CommandType = CommandType.Text;
        cmd.Connection = con;
        try
        {
            con.Open();
            sda.SelectCommand = cmd;
            sda.Fill(ds);
            return ds;
        }
        catch (Exception ex)
        {
            error = ex.Message + "||" + ex.StackTrace;
            return null;
        }
        finally
        {
            con.Close();
            sda.Dispose();
            con.Dispose();
        }
    }
    
    public int Insert_update_delete_param_query(string query, string[] parameternames, string[] parameterValues)
    {
        string connetionString = null;
        SqlConnection cnn;
        SqlCommand cmd;
        string sql = null;

        connetionString = System.Configuration.ConfigurationManager.ConnectionStrings["connectionString"].ConnectionString;
        sql = query;
        result = 0;
        cnn = new SqlConnection(connetionString);
        try
        {
            cnn.Open();
            cmd = new SqlCommand(sql, cnn);
            for (int i = 0; i < parameternames.Length; i++)
                cmd.Parameters.AddWithValue(parameternames[i], parameterValues[i]);
            cmd.ExecuteNonQuery();
            cmd.Dispose();
            cnn.Close();

        }
        catch (Exception ex)
        {
            result = -4;
            error = ex.Message + "||" + ex.StackTrace;
        }
        return result;
    }

    public DataSet Get_Page_Wise_Data(out int RecordCount, string[] parameternames, string[] parameterValues, string procName)
    {
        RecordCount = 0;
        using (SqlConnection con = new SqlConnection(connectionString))
        {
            using (SqlCommand cmd = new SqlCommand(procName, con))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                for (int i = 0; i < parameternames.Length && i < parameterValues.Length; i++)
                    cmd.Parameters.AddWithValue(parameternames[i], parameterValues[i]);
                cmd.Parameters.Add("@RecordCount", SqlDbType.Int, 4);
                cmd.Parameters["@RecordCount"].Direction = ParameterDirection.Output;
                SqlDataAdapter sda = new SqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                try
                {
                    con.Open();
                    sda.Fill(ds);
                    con.Close();
                    RecordCount = Convert.ToInt32(cmd.Parameters["@RecordCount"].Value);
                    return ds;
                }
                catch (Exception ex)
                {
                    error = ex.Message + "||" + ex.StackTrace;
                    return null;
                }
                finally
                {
                    con.Close();
                    sda.Dispose();
                    con.Dispose();
                }
            }
        }
    }
}
