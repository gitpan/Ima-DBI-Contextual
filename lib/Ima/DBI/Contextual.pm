
package Ima::DBI::Contextual;

use strict;
use warnings 'all';
use Carp 'confess';
use DBI;
use Digest::MD5 'md5_hex';

our $VERSION = '0.004';


my %contexts = ( );
  
sub set_db
{
  my ($pkg)           = shift;
  $pkg                = ref($pkg) ? ref($pkg) : $pkg;
  my ($name)          = shift;
  my @dsn_with_attrs  = @_;
  my @dsn             = grep { ! ref($_) } @_;
  my ($attrs)         = grep { ref($_) } @_;
  my $default_attrs   = {
		RaiseError => 1,
		AutoCommit => 0,
		PrintError => 0,
		Taint      => 1,
  };
  map { $attrs->{$_} = $default_attrs->{$_} unless defined($attrs->{$_}) }
    keys %$default_attrs;
  
  @dsn_with_attrs = ( @dsn, $attrs );

  no strict 'refs';
  no warnings 'redefine';
  *{"$pkg\::__dsn"} = sub { @dsn_with_attrs };
  *{"$pkg\::db_$name"} = $pkg->_mk_closure( @dsn_with_attrs );
  return;

}# end set_db()


sub _mk_closure
{
  my ($pkg, @dsn_with_attrs) = @_;
  
  return sub {
    my ($class) = @_;
    $class = ref($class) ? ref($class) : $class;
    
    my @dsn = $class->__dsn();
    my $key = $class->dbi_context( @dsn );
    if( my $context = $contexts{$key} )
    {
      if( $context->{dbh} && eval {$context->{dbh}->ping; 1;} )
      {
        return $context->{dbh};
      }
      else
      {
        $context->{dbh} = DBI->connect( @dsn );
        return $context->{dbh};
      }# end if()
    }
    else
    {
      $contexts{$key} = {
        dbh => DBI->connect_cached( @dsn )
      };
      return $contexts{$key}->{dbh};
    }# end if()
  };
}# end _mk_closure()


sub dbi_context
{
  my ($class, @info) = @_;
  
  my @parts = ( );
  foreach( @info )
  {
    if( ref($_) eq 'HASH' )
    {
      my $h = $_;
      push @parts, map {"$_=$h->{$_}"} sort keys %$h;
    }
    else
    {
      push @parts, $_;
    }# end if()
  }# end foreach()
  
  return md5_hex(join ", ", @parts);
}# end dbi_context()


sub dbi_ping
{
  my ($class, $dbh) = @_;
  
  eval { $dbh->ping; 1 }
}# end dbi_ping()


sub rollback
{
  my ($class) = @_;
  
  $class->db_Main->rollback;
}# end dbi_rollback()


sub commit
{
  my ($class) = @_;
  
  $class->db_Main->commit;
}# end dbi_commit()

1;# return true:

=pod

=head1 NAME

Ima::DBI::Contextual - Liteweight context-aware dbi handle cache and utility methods.

=head1 SYNOPSIS

  package Foo;
  
  use base 'DBIx::Connection::Cached';
  
  my @dsn = ( 'DBI:mysql:dbname:hostname', 'username', 'password', {
    RaiseError => 0,
  });
  __PACKAGE__->set_db('Main', @dsn);

Then, elsewhere:

  my $dbh = Foo->db_Main;

=head1 DESCRIPTION

If you like L<Ima::DBI> but need it to be more context-aware (eg: tie dbi connections to
more than the name and process id) then you need C<Ima::DBI::Contextual>.

B<Indications>: For permanent relief of symptoms related to hosting multiple mod_perl
web applications on one server, where each application uses a different database
but they all refer to the database handle via C<<Class->db_Main>>.  Such symptoms 
may include:

=over 4

=item * Wonky behavior which causes one website to fail because it's connected to the wrong database.

Scenario - Everything is going fine, you're clicking around walking your client through
a demo of the web application and then BLAMMO - B<500 server error>!  Another click and it's OK.  WTF?
You look at the log for Foo application and it says something like "C<Unknown method 'frobnicate' in package Bar::bozo>"

Funny thing is - you never connected to that database.  You have no idea B<WHY> it is trying to connect to that database.
Pouring over the guts in L<Ima::DBI> it's clear that L<Ima::DBI> only caches database
handles by Process ID (C<$$>) and name (eg: db_B<Main>).  So if the same Apache child
process has more than one application running within it and each application has C<db_Main> then 
I<it's just a matter of time before your application blows up>.

=item * Wondering for years what happened.

Years, no less.

=item * Not impressing your boss.

Yeah - it can happen - when you have them take a look at your new shumwidget and
instead of working - it I<doesn't> work.  All your preaching about unit tests and
DRY go right out the window when the basics (eg - connecting to the B<CORRECT FRIGGIN' DATABASE>) are broken.

=back

=head1 SEE ALSO

L<Ima::DBI>

=head1 AUTHOR

John Drago <jdrago_999@yahoo.com>

=head1 LICENSE

This software is B<Free> software and may be used and redistributed under the same
terms as Perl itself.

=cut
