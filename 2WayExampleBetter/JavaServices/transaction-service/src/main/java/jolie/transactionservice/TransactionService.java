package jolie.transactionservice;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import jolie.runtime.CanUseJars;
import jolie.runtime.FaultException;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.ValueVector;

/**
 * Represents a class which can connect to a single database, but then open
 * several connections to that database concurrently.
 * 
 * Some of the code has been 'inspired' by the Database service in the standard
 * library of Jolie:
 * 'https://github.com/jolie/jolie/blob/master/javaServices/coreJavaServices/src/main/java/joliex/db/DatabaseService.java'
 */

@CanUseJars({
        "jdbc-postgresql.jar", // PostgreSQL is the one I use for now.
        "jdbc-sqlite.jar" // SQLite
})
public class TransactionService extends JavaService {

    /**
     * Reasons for ConcurrentHashMap:
     * 
     * Multiple threads might attempt to put something into the map at the same
     * time, which could constitude a problem (?).
     * Don't really think it would, as I'm never iterating over elements
     * in the map, but I also don't know the internals of the 'put' and
     * 'remove' functions, so better be safe
     * 
     * Connections should theoretically be inherintly thread safe, so handling the
     * con object of the map should be alright, as long as
     * it is understood that once 'commit' is called, all un-executed statements are
     * closed without being executed.
     * Therefore, this class should not be called in parallel like:
     * 
     * parrallel{
     * executeUpdate@TransactionService()() | commit@TransactionService()()
     * }
     * ^^^UNSAFE^^^
     * 
     * parrallel{
     * executeUpdate@TransactionService()() | executeUpdate@TransactionService()()
     * }
     * ^^^SAFE(ish)^^^
     */
    private ConcurrentHashMap<String, java.sql.Connection> m_openTransactions = new ConcurrentHashMap<String, java.sql.Connection>();

    private String m_connectionString = null;
    private String m_username = null;
    private String m_password = null;
    private String m_driver = null;
    private String m_driverClass = null;

    // This lock ensures ensures that we never end in the situation where two
    // threads manipulate the same connection
    // in such a way that the same transaction is first aborted, then committed.
    // e.g. First 'con' is retrieved by thread A(commit), then 'con' is aborted by
    // thread B(abort), and then 'con' is comitted by A.
    private Object m_commitAbortLock = new Object();

    /**
     * Sets or overwrites the current connection string, meaning which database any
     * writes are made to.
     * 
     * @param request
     * @throws FaultException
     */
    public Value connect(Value request) throws FaultException {
        System.out.println("Transactionservice: Connecting");
        Value response = Value.create();

        // If connectionString is set, this service is already connected to a database
        if (m_connectionString != null) {
            response.setValue("Connected to TransactionService");
        } else {

            // Retrieve all the information stored in the request parameter
            m_driver = request.getChildren("driver").first().strValue();
            if (request.getFirstChild("driver").hasChildren("class")) {
                m_driverClass = request.getFirstChild("driver").getFirstChild("class").strValue();
            }
            String host = request.getChildren("host").first().strValue();
            String port = request.getChildren("port").first().strValue();
            String databaseName = request.getChildren("database").first().strValue();
            m_username = request.getChildren("username").first().strValue();
            m_password = request.getChildren("password").first().strValue();
            String attributes = request.getFirstChild("attributes").strValue();
            String separator = File.separator;
            Optional<String> encoding = Optional
                    .ofNullable(
                            request.hasChildren("encoding") ? request.getFirstChild("encoding").strValue() : null);

            // Load the corrrect driver if it is defined
            try {
                if (m_driverClass != null) {
                    System.out.println("Loading driver: " + m_driver);
                    Class.forName(m_driverClass);
                } else {
                    switch (m_driver) {
                        case "postgresql":
                            Class.forName("org.postgresql.Driver");
                            break;
                        case "sqlite":
                            Class.forName("org.sqlite.JDBC");
                            break;
                        default:
                            throw new FaultException("InvalidDriver", "Unknown type of driver: " + m_driver);
                    }
                }

                // Construct the connectionString
                if (m_driver.equals("sqlite")) {
                    m_connectionString = "jdbc:" + m_driver + ":" + databaseName;
                    if (!attributes.isEmpty()) {
                        m_connectionString += ";" + attributes;
                    }
                } else // m_driver is postgresql
                {
                    m_connectionString = "jdbc:" + m_driver + "://" + host + (port.isEmpty() ? "" : ":" + port)
                            + separator + databaseName;
                    if (encoding.isPresent()) {
                        m_connectionString += "?characterEncoding=" + encoding.get();
                    }
                }
                response.setValue("Connected to TransactionService");
            } catch (ClassNotFoundException e) {
                // Thrown if trying to load a driver which is not in the classpath
                throw new FaultException("DriverClassNotFound", e);
            }
        }
        System.out.println("Transactionservice: Connected");
        return response;
    }

    public Value initializeTransaction() throws FaultException {
        System.out.println("Transactionservice: initializing new transaction");
        Value response = Value.create();
        Connection con;
        try {
            // Create a new connection, and map it to a generated UUID.
            con = DriverManager.getConnection(
                    m_connectionString,
                    m_username,
                    m_password);
            con.setAutoCommit(false); // This line is where the magic happens
            String uuid = UUID.randomUUID().toString();

            // Store the open transactions in a map. uuid is used as a handle.
            m_openTransactions.put(uuid, con);

            response.setValue(uuid);
            System.out.println("Transactionservice: New transaction initialized");
            return response;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new FaultException("SQLException", "Error while starting transaction.");
        }
    }

    public Value executeQuery(Value input) throws FaultException {
        System.out.println("Transactionservice: Executing query");
        String transactionHandle = input.getFirstChild("handle").strValue();
        String query = input.getFirstChild("query").strValue();

        Value response = Value.create();

        try {
            // Execute a query on a given connection
            Connection con = m_openTransactions.get(transactionHandle);

            if (con == null) {
                throw new FaultException("ConnectionClosed",
                        "No connection with transaction handle " + transactionHandle + " is open.");
            }

            PreparedStatement statement = con.prepareStatement(query);
            ResultSet result = statement.executeQuery();

            // Load the result of the query into the response Jolie variable.
            resultSetToValueVector(result, response.getChildren("row"));
            System.out.println("Transactionservice: Executed query");
            return response;
        } catch (SQLException e) {
            throw new FaultException("SQLException", e);
        }
    }

    public Value executeUpdate(Value input) throws FaultException {
        String transactionHandle = input.getFirstChild("handle").strValue();
        String query = input.getFirstChild("update").strValue();

        Value response = Value.create();
        try {
            // Execute an update on an open connection.
            Connection con = m_openTransactions.get(transactionHandle);

            if (con == null) {
                throw new FaultException("ConnectionClosed",
                        "No connection with transaction handle " + transactionHandle + " is open.");
            }

            PreparedStatement statement = con.prepareStatement(query);
            int numberRowsUpdated = statement.executeUpdate();
            // Return the number of rows affected by the update
            response.setValue(numberRowsUpdated);
            return response;
        } catch (SQLException e) {
            System.out.println("Query: '" + query + "' failed");
            throw new FaultException("SQLException", e);
        }
    }

    public Value commit(String transactionHandle) throws FaultException {
        Value response = Value.create();

        try {
            synchronized (m_commitAbortLock) {
                Connection con = m_openTransactions.get(transactionHandle);

                if (con == null) {
                    throw new FaultException("ConnectionClosed",
                            "No connection with transaction handle " + transactionHandle + " is open.");
                }

                // Commit the transaction, then remove it from the open transactions
                con.commit();
                m_openTransactions.remove(transactionHandle);

                response.setValue("Transaction " + transactionHandle + " was commited sucessfully.");
                System.out.println("Transactionservice: Committed transaction");
                return response;
            }
        } catch (SQLException e) {
            throw new FaultException("SQLException", e);
        }
    }

    public boolean abort(String transactionHandle) throws FaultException {
        try {
            Connection con;
            synchronized (m_commitAbortLock) {
                con = m_openTransactions.get(transactionHandle);

                if (con == null) {
                    throw new FaultException("ConnectionClosed",
                            "No connection with transaction handle " + transactionHandle + " is open.");
                }

                con.rollback();
                con.close();
                m_openTransactions.remove(transactionHandle);
                return con.isClosed();
            }
        } catch (SQLException e) {
            throw new FaultException("SQLException", e);
        }
    }

    // *-------------------------- Private fucntions -------------------------- */
    /**
     * Fills reach row in the matrix vector with data from corresponding entries in
     * result. Basically, copy the values from a ResultSet into a Jolie ValueVector.
     * 
     * @param result - The ResultSet to copy from
     * @param vector - The ValueVector to fill.
     * @throws SQLException - Thrown if any operation on result fails.
     */
    private static void resultSetToValueVector(ResultSet result, ValueVector vector)
            throws SQLException {
        Value rowValue, fieldValue;
        ResultSetMetaData metadata = result.getMetaData();
        int cols = metadata.getColumnCount();
        int i;
        int rowIndex = 0;

        // As opposed to the Database service, for this simple example, we don't support
        // ToUpper and ToLower.

        while (result.next()) {
            rowValue = vector.get(rowIndex);
            for (i = 1; i <= cols; i++) {
                fieldValue = rowValue.getFirstChild(metadata.getColumnLabel(i));
                setValue(fieldValue, result, metadata.getColumnType(i), i);
            }
            rowIndex++;
        }
    }

    /**
     * Sets the value of fieldValue to the corresponding field in result. This
     * functions purely as a sort of 'parsing', it seems.
     * I've removed support for non-integer values for the sake of
     * reading-simplicity. Also, don't ask me why they used a switch statement here,
     * but not in the 'connect' method.
     * 
     * So glad I didn't have to write this by hand - I yoinked it directly from the
     * Database Service of jolie
     * 
     * @throws SQLException - Thrown if any operation on result fails.
     */
    private static void setValue(Value fieldValue, ResultSet result, int columnType, int index)
            throws SQLException {
        switch (columnType) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                fieldValue.setValue(result.getInt(index));
                break;
            case java.sql.Types.BIGINT:
                fieldValue.setValue(result.getLong(index));
                break;
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGNVARCHAR:
                String s = result.getNString(index);
                if (s == null) {
                    s = "";
                }
                fieldValue.setValue(s);
                break;
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                fieldValue.setValue(result.getBoolean(index));
                break;
            case java.sql.Types.VARCHAR:
            default:
                String str = result.getString(index);
                if (str == null) {
                    str = "";
                }
                fieldValue.setValue(str);
                break;
        }
    }

}