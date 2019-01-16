package cqelsplus.execplan.data;


public class DomRingEntry extends DomEntry 
{
	
	private LinkedItem link;
	
	public synchronized void setLink(LinkedItem link)
	{ 
		this.link=link;
	}
	
	@Override
	public PoolableObject newObject()
	{
		return (PoolableObject)POOL.DomRingEntry.borrowObject();
	}
	
	@Override
	public synchronized LinkedItem getLink() {
		return link;
	}
	
	@Override
	public synchronized void releaseInstance() {
		POOL.DomRingEntry.returnObject(this);
	}
}
