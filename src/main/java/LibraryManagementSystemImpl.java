import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.sql.*;
import java.util.List;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        try(Statement stmt = connector.getConn().createStatement();){
            try (ResultSet rset = stmt.executeQuery("select category,title,press,publish_year,author from book");){
                while(rset.next()){
                    String category = rset.getString("category");  
                    String title = rset.getString("title"); 
                    String author = rset.getString("author");
                    String press = rset.getString("press");
                    int publish_year = rset.getInt("publish_year");
                    if(category == book.getCategory()&&title==book.getTitle()&&press==book.getPress()&&author == book.getAuthor()&&publish_year==book.getPublishYear()){
                        return new ApiResult(false, "duplicate book_info");
                    }    
                }
                String sql = "insert into book(category,title,press,publish_year,author,price,stock) values(?,?,?,?,?,?,?)";
                try (PreparedStatement pstmt = connector.getConn().prepareStatement(sql)) {
                    pstmt.setString(1, book.getCategory());
                    pstmt.setString(2, book.getTitle()); 
                    pstmt.setString(3, book.getPress());
                    pstmt.setInt(4, book.getPublishYear());
                    pstmt.setString(5, book.getAuthor());
                    pstmt.setDouble(6, book.getPrice());
                    pstmt.setInt(7, book.getStock());     
                    pstmt.executeQuery();
                    String id = "select max(book_id) from book";
                    ResultSet rs_id = stmt.executeQuery(id);
                    if(rs_id.next()){
                        int b_id = rs_id.getInt(1);
                        book.setBookId(b_id);
                    }
                }
            
            } catch (Exception e) {
                
            }
        }catch(Exception e){
            System.out.println("创建 Statement 时发生错误: " + e.getMessage());
        }
        
        return new ApiResult(true, "store action complete");
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult removeBook(int bookId) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult registerCard(Card card) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult removeCard(int cardId) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult showCards() {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
