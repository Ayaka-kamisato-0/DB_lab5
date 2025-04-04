import entities.Book;
import entities.Borrow;
import entities.Card;
import entities.Card.CardType;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.sql.*;
import java.util.List;
import java.util.Set;

import javax.print.attribute.standard.PresentationDirection;

import org.postgresql.jdbc2.ArrayAssistantRegistry;

import java.util.ArrayList;
import java.util.HashSet;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
public ApiResult storeBook(Book book) {
    Connection conn = connector.getConn();
    int bookId = 0;
    try {
    // 先检查是否存在重复的书籍
    String checkSql = "SELECT book_id FROM book WHERE category = ? AND title = ? AND press = ? AND author = ? AND publish_year = ?";
    
    PreparedStatement checkStmt = connector.getConn().prepareStatement(checkSql);
        checkStmt.setString(1, book.getCategory());
        checkStmt.setString(2, book.getTitle());
        checkStmt.setString(3, book.getPress());
        checkStmt.setString(4, book.getAuthor());
        checkStmt.setInt(5, book.getPublishYear());
        
        ResultSet rs = checkStmt.executeQuery();
            if (rs.next()) {
                return new ApiResult(false, "duplicate book_info");
            }


    // 插入新书
    PreparedStatement pstmt = conn.prepareStatement("INSERT INTO book (category, title, press, publish_year, author, price, stock) VALUES (?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
    
        pstmt.setString(1, book.getCategory());
        pstmt.setString(2, book.getTitle());
        pstmt.setString(3, book.getPress());
        pstmt.setInt(4, book.getPublishYear());
        pstmt.setString(5, book.getAuthor());
        pstmt.setDouble(6, book.getPrice());
        pstmt.setInt(7, book.getStock());
        pstmt.executeUpdate();

        // 获取自增生成的book_id
        ResultSet generatedKeys = pstmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                bookId = generatedKeys.getInt(1);
                book.setBookId(bookId);
            } 

        commit(conn);
        return new ApiResult(true, bookId);

 
}catch(Exception e){
    rollback(conn);
    return new ApiResult(false, null);
}

}



@Override
public ApiResult incBookStock(int bookId, int deltaStock) {
    Connection conn = connector.getConn();
        String stock = "select stock from book where book_id = ?";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(stock)){
            pstmt.setInt(1, bookId);
            ResultSet rs_stock = pstmt.executeQuery();
            
            if(rs_stock.next()){
                int stock_num = rs_stock.getInt("stock");
                if(stock_num+deltaStock>=0){
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
            commit(conn);
            return new ApiResult(true, "success");
        }catch(Exception e){
            rollback(conn);
            return new ApiResult(true, "updated stock num");
        }
}


    @Override
    public ApiResult storeBook(List<Book> books) {
        Set<String> seen = new HashSet<>();
        for (Book book : books) {
            String key = book.getAuthor() + "|" + book.getTitle()+"|"+book.getCategory()+"|"+book.getPress()+"|"+String.valueOf(book.getPublishYear()); 
            if (!seen.add(key)) { 
                return new ApiResult(false, "duplicate in batch1");
            }
        }
        try(Statement stmt = connector.getConn().createStatement()){
            try (ResultSet rset = stmt.executeQuery("select * from book")){
                while(rset.next()){
                    String key = rset.getString("author") + "|" + rset.getString("title")+"|"+rset.getString("category")+"|"+rset.getString("press")+"|"+String.valueOf(rset.getInt("publish_year")); 
                if (!seen.add(key)) { 
                    return new ApiResult(false, "duplicate in batch2");
                }
                }
            }catch(Exception e){

            }
        }catch (Exception e) {

        }
        try{
            for(Book book:books){
                String insertSql = "INSERT INTO book (category, title, press, publish_year, author, price, stock) VALUES (?, ?, ?, ?, ?, ?, ?)";
                int bookId= 0;
            try (PreparedStatement pstmt = connector.getConn().prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, book.getCategory());
            pstmt.setString(2, book.getTitle());
            pstmt.setString(3, book.getPress());
            pstmt.setInt(4, book.getPublishYear());
            pstmt.setString(5, book.getAuthor());
            pstmt.setDouble(6, book.getPrice());
            pstmt.setInt(7, book.getStock());
            pstmt.executeUpdate();

        // 获取自增生成的book_id
        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
            if (generatedKeys.next()) {
                bookId = generatedKeys.getInt(1);
                book.setBookId(bookId);
            } else {
                rollback(connector.getConn());
                return new ApiResult(false, "Failed to retrieve generated book ID.");
            }
        }
    } catch (SQLException e) {
        return new ApiResult(false, "Error storing book: " + e.getMessage());
    }
            }
            commit(connector.getConn());
        }catch(Exception e){
            rollback(connector.getConn());
            return new ApiResult(false, null);
        }
        

    return new ApiResult(true, "batch store completed");
}
    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        try (PreparedStatement pstmt = connector.getConn().prepareStatement("select * from book where book_id = ?")){
            pstmt.setInt(1, bookId);
            ResultSet rset = pstmt.executeQuery();
            if(!rset.next()){
                return new ApiResult(false, "book not exists,remove failed");
            }
        } catch (Exception e) {

        }
        String check = "select * from borrow where book_id = ? and return_time = 0";
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
        commit(conn);
        return new ApiResult(true, "deletion success");
    }


    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        int book_id = book.getBookId();
        String category = book.getCategory();
        String title = book.getTitle();
        String press = book.getPress();
        int publish_year = book.getPublishYear();
        String author = book.getAuthor();
        double price = book.getPrice();

        String check = "select book_id,stock from book where book_id = ? ";
        try(PreparedStatement pstmt = connector.getConn().prepareStatement(check)){
            pstmt.setInt(1, book_id);
            ResultSet rset = pstmt.executeQuery();
            if(rset.next()){
                //if(rset.getInt("stock")!=book.getStock()){
                //    return new ApiResult(false, "stock can not be modified");
                //}
                String modify = "update book set category = ?,title = ?,press = ?,publish_year = ?,author = ?,price = ? where book_id = ?";
                try(PreparedStatement ptmt1 = connector.getConn().prepareStatement(modify)){
                    ptmt1.setString(1, category);
                    ptmt1.setString(2, title);
                    ptmt1.setString(3, press);
                    ptmt1.setInt(4, publish_year);
                    ptmt1.setString(5, author);
                    ptmt1.setDouble(6, price);
                    ptmt1.setInt(7, book_id);
                    ptmt1.execute();
                }
            }else{
                return new ApiResult(false, "book not exists");
            }
        }catch(Exception e){

        }
        commit(conn);
        return new ApiResult(true, "modify completed");
    }


    
    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        List<Book> res = new ArrayList<>();
        String category = conditions.getCategory()==null?"%":conditions.getCategory();
        String title = conditions.getTitle()==null?"%":conditions.getTitle();
        String press = conditions.getPress()==null?"%":conditions.getPress();
        String author  = conditions.getAuthor()==null?"%":conditions.getAuthor();
        int minyear = conditions.getMinPublishYear()==null?Integer.MIN_VALUE:conditions.getMinPublishYear();
        int maxyear = conditions.getMaxPublishYear()==null?Integer.MAX_VALUE:conditions.getMaxPublishYear();
        double minprice = conditions.getMinPrice()==null?-Double.MAX_VALUE:conditions.getMinPrice();
        double maxprice = conditions.getMaxPrice()==null?Double.MAX_VALUE:conditions.getMaxPrice();
        String str = "select * from book where category like ? and title like ? and press like ? and author like ? and (publish_year between ? and ? )and (price between ? and ?)";
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
                    String category_str = rset.getString("category");
                    int id = rset.getInt("book_id");
                    String title_str = rset.getString("title");
                    String press_str  =rset.getString("press");
                    int pub = rset.getInt("publish_year");
                    String author_str = rset.getString("author");
                    double price = rset.getDouble("price");
                    int stock = rset.getInt("stock");
                    Book book = new Book(category_str,title_str,press_str,pub,author_str,price,stock);
                    book.setBookId(id);
                    res.add(book);
                }
            }
            if(conditions.getSortOrder()==queries.SortOrder.ASC)
            {res.sort(conditions.getSortBy().getComparator().thenComparing(Book::getBookId));}
            else {res.sort(conditions.getSortBy().getComparator().reversed().thenComparing(Book::getBookId));}
        }catch(Exception e){

        }

        return new ApiResult(true,new queries.BookQueryResults(res));
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try {
            
            PreparedStatement stmt = conn.prepareStatement("select stock from book where book_id=? for update;");
            stmt.setInt(1, borrow.getBookId());
            ResultSet rset = stmt.executeQuery();
            stmt=conn.prepareStatement("select * from card where card_id=?;");
            stmt.setInt(1, borrow.getCardId());
            ResultSet rset1=stmt.executeQuery();
            if (rset.next()&&rset1.next()) {
                int stock = rset.getInt(1);
                if (stock <= 0) {
                    rollback(conn);
                    return new ApiResult(false, "no book");
                }
                
                stmt = conn.prepareStatement("select * from borrow where book_id=? and card_id=? and return_time=0;");
                stmt.setInt(1, borrow.getBookId());
                stmt.setInt(2, borrow.getCardId());
                rset = stmt.executeQuery();
                if (rset.next()) {
                    rollback(conn);
                    return new ApiResult(false, "you have borrow it;");
                }
                
                stmt = conn.prepareStatement("insert into borrow(card_id,book_id,borrow_time,return_time) values(?,?,?,?);");
                stmt.setInt(1, borrow.getCardId());
                stmt.setInt(2, borrow.getBookId());
                stmt.setLong(3, borrow.getBorrowTime());
                stmt.setLong(4, 0);
                stmt.executeUpdate();
                
                stmt = conn.prepareStatement("update book set stock = stock - 1 where book_id=?;");
                stmt.setInt(1, borrow.getBookId());
                stmt.executeUpdate();
                commit(conn);
                return new ApiResult(true, "success");
            } else {
                rollback(conn);
                return new ApiResult(false, "no book");
            }
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn=connector.getConn();
        try{
            PreparedStatement stmt=conn.prepareStatement("select borrow_time from borrow where book_id=? and card_id=? and return_time=? for update;");
            stmt.setInt(1, borrow.getBookId());
            stmt.setInt(2, borrow.getCardId());
            stmt.setInt(3, 0);
            ResultSet rset=stmt.executeQuery();
            if(rset.next()){
                int stock;
                if(borrow.getReturnTime()<=rset.getLong(1)){
                    rollback(conn);
                    return new ApiResult(false,"fail");
                }
                
                stmt=conn.prepareStatement("update book set stock=stock+1 where book_id=?;");
                
                stmt.setInt(1, borrow.getBookId());
                stmt.executeUpdate();
                stmt=conn.prepareStatement("update borrow set return_time=? where card_id=? and book_id=? and return_time=?;");
                stmt.setLong(1, borrow.getReturnTime());
                stmt.setInt(2, borrow.getCardId());
                stmt.setInt(3, borrow.getBookId());
                stmt.setInt(4, 0);
                stmt.executeUpdate();
                commit(conn);
                return new ApiResult(true, "success");
            }
            else{
                return new ApiResult(false, "have return book");
            }
        } 
        catch(Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }



    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn=connector.getConn();
        try{
            List<BorrowHistories.Item>items=new ArrayList<>();
            
            PreparedStatement stmt=conn.prepareStatement("select card_id,book_id,category,title,press,publish_year,author,price,borrow_time,return_time from book natural join borrow where card_id=? order by borrow_time desc,book_id asc;");
            stmt.setInt(1, cardId);
            ResultSet rset=stmt.executeQuery();

            while(rset.next()){
                BorrowHistories.Item item=new BorrowHistories.Item();
                item.setCardId(rset.getInt(1));
                item.setBookId(rset.getInt(2));
                item.setCategory(rset.getString(3));
                item.setTitle(rset.getString(4));
                item.setPress(rset.getString(5));
                item.setPublishYear(rset.getInt(6));
                item.setAuthor(rset.getString(7));
                item.setPrice(rset.getDouble(8));
                item.setBorrowTime(rset.getLong(9));
                item.setReturnTime(rset.getLong(10));
                items.add(item);
            }
            commit(conn);
            return new ApiResult(true, "success",new BorrowHistories(items));
        }catch(Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }


    @Override
    public ApiResult registerCard(Card card) {
        Connection conn=connector.getConn();
        try{
            PreparedStatement stmt=conn.prepareStatement("select * from card where name=? and department=? and type=?;");
            stmt.setString(1, card.getName());
            stmt.setString(2, card.getDepartment());
            stmt.setString(3, card.getType().getStr());
            ResultSet rset=stmt.executeQuery();
            if(rset.next()){
                rollback(conn);
                return new ApiResult(false, "already exist");
            }
            stmt=conn.prepareStatement("insert into card(name,department,type) values (?,?,?);");
            stmt.setString(1, card.getName());
            stmt.setString(2, card.getDepartment());
            stmt.setString(3, card.getType().getStr());
            stmt.executeUpdate();
            stmt=conn.prepareStatement("select * from card where name=? and department=? and type=?;");
            stmt.setString(1, card.getName());
            stmt.setString(2, card.getDepartment());
            stmt.setString(3, card.getType().getStr());
            rset=stmt.executeQuery();
            while(rset.next()){
            card.setCardId(rset.getInt(1));
            }
            commit(conn);
            return new ApiResult(true, "success");
        }catch(Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn=connector.getConn();
        try{
            PreparedStatement stmt=conn.prepareStatement("select * from borrow where card_id=? and return_time=? for update;");
            stmt.setInt(1, cardId);
            stmt.setInt(2, 0);
            ResultSet rset=stmt.executeQuery();
            if(rset.next()){
                rollback(conn);
                return new ApiResult(false, "have book not return;");
            }
            stmt=conn.prepareStatement("delete from card where card_id=?;");
            stmt.setInt(1, cardId);
            int t1=stmt.executeUpdate();
            stmt=conn.prepareStatement("delete from borrow where card_id=?;");
            stmt.setInt(1, cardId);
            stmt.executeUpdate();
            if(t1==0){
                rollback(conn);
                return new ApiResult(false, "defeat");
            }
            commit(conn);
            return new ApiResult(true, "success");
        }catch(Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult showCards() {
        Connection conn=connector.getConn();
        try{
            List<Card>cards=new ArrayList<>();
            PreparedStatement stmt=conn.prepareStatement("select card_id,name,department,type from card order by card_id asc;");
            ResultSet rset=stmt.executeQuery();
            while(rset.next()){
                Card card = new Card();
                card.setCardId(rset.getInt(1));
                card.setName(rset.getString(2));
                card.setDepartment(rset.getString(3));
                card.setType(Card.CardType.values(rset.getString(4)));
                cards.add(card);
                
            }
            commit(conn);
                return new ApiResult(true, "success",new CardList(cards));

        }catch(Exception e){
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
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
