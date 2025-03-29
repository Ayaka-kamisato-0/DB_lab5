import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils.Null;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        try(Statement stmt = connector.getConn().createStatement()){
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
                    pstmt.execute();
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
        try(Statement stmt = connector.getConn().createStatement()){
            String stock = "select stock from book where book_id = ?";
            try(PreparedStatement pstmt = connector.getConn().prepareStatement(stock)){
                pstmt.setInt(1, bookId);
                ResultSet rs_stock = pstmt.executeQuery();
                int stock_num;
                if(rs_stock.next()){
                    stock_num = rs_stock.getInt(1);
                    if(stock_num+deltaStock>0){
                        String update = "update book set stock = ? where book_ID = ?";
                        try(PreparedStatement pstmt2 = connector.getConn().prepareStatement(update)){
                            pstmt2.setInt(1, stock_num+deltaStock);
                            pstmt2.setInt(2, bookId);
                            pstmt2.execute();
                        }
                    }else{
                        return new ApiResult(false, "invalid stock num");
                    }
                }else{
                    return new ApiResult(false, "book not found");
                }

            }
        }catch(Exception e){

        }
        return new ApiResult(true, "updated stock num");
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        
        try(Statement stmt = connector.getConn().createStatement()){
            for(Book book:books){
            try (ResultSet rset = stmt.executeQuery("select category,title,press,publish_year,author from book")){
                
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
            }catch(Exception e){

            }
        }
    try{
    for(Book book:books){
                String sql = "insert into book(category,title,press,publish_year,author,price,stock) values(?,?,?,?,?,?,?)";
                try (PreparedStatement pstmt = connector.getConn().prepareStatement(sql)) {
                    pstmt.setString(1, book.getCategory());
                    pstmt.setString(2, book.getTitle()); 
                    pstmt.setString(3, book.getPress());
                    pstmt.setInt(4, book.getPublishYear());
                    pstmt.setString(5, book.getAuthor());
                    pstmt.setDouble(6, book.getPrice());
                    pstmt.setInt(7, book.getStock());     
                    pstmt.execute();
                    String id = "select max(book_id) from book";
                    ResultSet rs_id = stmt.executeQuery(id);
                    if(rs_id.next()){
                        int b_id = rs_id.getInt(1);
                        book.setBookId(b_id);
                    }
                }
            }
        }catch(Exception e){
            rollback(connector.getConn());
        }
        
    }catch(Exception e){

    }
    return new ApiResult(true, "batch store completed");
}


    @Override
    public ApiResult removeBook(int bookId) {
        String check = "select book_id from borrow where book_id = ? and return_time = 0";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(check)){
            pstmt.setInt(1, bookId);
            ResultSet rset = pstmt.executeQuery();
            if(rset.next()){
                return new ApiResult(false, "borrowed, deletion failed");
            }
            else{
                String del = "delete from book where book_id = ?";
                try(PreparedStatement pstmt1 = connector.getConn().prepareStatement(del)){
                    pstmt1.setInt(1, bookId);
                    try{
                        pstmt1.execute();
                    }catch(Exception e){
                        return new ApiResult(false, "deletion failed");
                    }
                }
            }
        }catch(Exception e){

        }
        return new ApiResult(true, "deletion success");
    }

    @Override //不能修改书号和存量
    public ApiResult modifyBookInfo(Book book) {
        //int book_id = book.getBookId();
        String category = book.getCategory();
        String title = book.getTitle();
        String press = book.getPress();
        int publish_year = book.getPublishYear();
        String author = book.getAuthor();
        double price = book.getPrice();
        int stock = book.getStock();
        String check = "select book_id,stock from book where book_id = ?";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(check)){
            pstmt.setInt(1, book.getBookId());
            ResultSet rset = pstmt.executeQuery();
            if(rset.next()){
                if(rset.getInt("stock")!=stock){
                    return new ApiResult(false, "stock can not be modified");
                }
                String modify = "update book set category = ?,title = ?,press = ?,publish_year = ?,author = ?,price = ? where book_id = ?";
                try(PreparedStatement ptmt1 = connector.getConn().prepareStatement(modify)){
                    ptmt1.setString(1, category);
                    ptmt1.setString(2, title);
                    ptmt1.setString(3, press);
                    ptmt1.setInt(4, publish_year);
                    ptmt1.setString(5, author);
                    ptmt1.setDouble(6, price);
                    ptmt1.execute();
                }
            }else{
                return new ApiResult(false, "book not exists");
            }
        }catch(Exception e){

        }
        return new ApiResult(true, "modify completed");
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        List<Book> res = new ArrayList<>();
        String category = conditions.getCategory();
        String title = conditions.getTitle();
        String press = conditions.getPress();
        String author  = conditions.getAuthor();
        int minyear = conditions.getMinPublishYear();
        int maxyear = conditions.getMaxPublishYear();
        double minprice = conditions.getMinPrice();
        double maxprice = conditions.getMaxPrice();
        String str = "select * from book where category = ? and (title like ? or press like ? or author like ?) and publish_year between ? and ? and price between ? and ?";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(str)){
            pstmt.setString(1, category);
            pstmt.setString(2, "%" +title+"%");
            pstmt.setString(3, "%"+press+"%");
            pstmt.setString(4, "%"+author+"%");
            pstmt.setInt(5, minyear);
            pstmt.setInt(6, maxyear);
            pstmt.setDouble(7, minprice);
            pstmt.setDouble(8, maxprice);
            try(ResultSet rset = pstmt.executeQuery()){
                while(rset.next()){
                    res.add(new Book(rset.getString("category"),rset.getString("title"),rset.getString("press"),rset.getInt("publish_year"),rset.getString(""),rset.getDouble("price"),rset.getInt("stock")));
                }
            }
            if(conditions.getSortOrder().getValue()=="asc")
            res.sort(conditions.getSortBy().getComparator().thenComparing(Book::getBookId));
            else res.sort(conditions.getSortBy().getComparator().reversed().thenComparing(Book::getBookId));
        }catch(Exception e){

        }

        return new ApiResult(true,res);
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        String sql = "select * from borrow where card_id = ? and book_id =?";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(sql)){
            pstmt.setInt(1, borrow.getCardId());
            pstmt.setInt(2, borrow.getBookId());
            ResultSet rset = pstmt.executeQuery();
            if(rset.next())return new ApiResult(false, "already borrowed");
        }catch(Exception e){

        }
        ApiResult api = incBookStock(borrow.getBookId(), -1);
        if(api.ok==false)return new ApiResult(false, "borrow failed due to stock reasons");
        String borrow_str = "insert into borrow values(?,?,?,?)";
        try(PreparedStatement pstmt1 = connector.getConn().prepareStatement(borrow_str)){
            pstmt1.setInt(1,borrow.getCardId());
            pstmt1.setInt(2,borrow.getBookId());
            pstmt1.setLong(3,borrow.getBorrowTime());
            //pstmt1.setLong(4,0);
            pstmt1.execute();
        }catch(Exception e){

        }
        return new ApiResult(true, "borrow completed");
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        String return_str = "update borrow set return_time = ?where card_id = ? and book_id = ?";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(return_str)) {
            pstmt.setLong(1, borrow.getReturnTime());
            pstmt.setInt(2, borrow.getCardId());
            pstmt.setInt(3, borrow.getBookId());
            try {
                pstmt.execute();
            } catch (Exception e) {
                rollback(connector.getConn());
                return new ApiResult(false,"return book failed");
            }
            incBookStock(borrow.getBookId(), 1);
        } catch (Exception e) {
            
        }
        return new ApiResult(true, "return completed");
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        List<BorrowHistories.Item> items = new ArrayList<>();
        String show_str ="select * from book natural join borrow where card_id = ?"; 
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(show_str)){
            pstmt.setInt(1, cardId);
            ResultSet rset = pstmt.executeQuery();
            while(rset.next()){
                Borrow bt = new Borrow(0,0);
                bt.setBorrowTime(rset.getLong("borrow_time"));
                bt.setReturnTime(rset.getLong("return_time"));
                items.add(new BorrowHistories.Item(rset.getInt("card_id"),new Book(rset.getString("category"),rset.getString("title"),rset.getString("Press"),rset.getInt("publish_year"),rset.getString("author"),rset.getDouble("price"),0),bt));
            }
            items.sort(Comparator.comparing(BorrowHistories.Item::getBorrowTime).reversed().thenComparing(BorrowHistories.Item::getBookId));
        } catch (Exception e) {

        }
        
        return new ApiResult(true,new BorrowHistories(items));
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
