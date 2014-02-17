import groovy.transform.CompileStatic;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.groovy.GroovyQueryScript;

@CompileStatic
class Hello implements GroovyQueryScript {
  def void handle(RowPipe pipe, RowBatch rowBatch)  {
    for (Row row : rowBatch.rows)
		row['groovy'] == 'pass'
		
	pipe.pushPipe(rowBatch)
  }
}