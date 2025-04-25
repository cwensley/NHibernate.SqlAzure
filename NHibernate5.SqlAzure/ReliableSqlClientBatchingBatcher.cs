// Parts of this file were copied from NHibernate.AdoNet.SqlClientBatchingBatcherFactory, but modified to use ReliableSqlDbConnection
// The #regions indicate the copied code
using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.AdoNet;
using NHibernate.AdoNet.Util;
using NHibernate.Driver;
using NHibernate.Exceptions;

namespace NHibernate.SqlAzure
{
    /// <summary>
    /// Exposes <see cref="SqlClientBatchingBatcher"/> functionality when a <see cref="ReliableSqlDbConnection"/>
    /// connection is being used.
    /// </summary>
    public class ReliableSqlClientBatchingBatcher : SqlClientBatchingBatcher
    {
        #region Impersonate private fields in base class
        private readonly ConnectionManager _connectionManager;
        private readonly FieldInfo _totalExpectedRowsAffectedField = typeof(SqlClientBatchingBatcher)
            .GetField("_totalExpectedRowsAffected", BindingFlags.NonPublic | BindingFlags.Instance);
        private readonly FieldInfo _currentBatchField = typeof (SqlClientBatchingBatcher)
            .GetField("_currentBatch", BindingFlags.NonPublic | BindingFlags.Instance);
        private readonly FieldInfo _currentBatchCommandsLogField = typeof(SqlClientBatchingBatcher)
            .GetField("_currentBatchCommandsLog", BindingFlags.NonPublic | BindingFlags.Instance);
        private readonly MethodInfo _createConfiguredBatchMethod = typeof (SqlClientBatchingBatcher)
            .GetMethod("CreateConfiguredBatch", BindingFlags.Instance | BindingFlags.NonPublic);
        private readonly MethodInfo _clearCurrentBatch = typeof(SqlClientBatchingBatcher)
            .GetMethod("ClearCurrentBatch", BindingFlags.Instance | BindingFlags.NonPublic);

		// ReSharper disable InconsistentNaming
		private int _totalExpectedRowsAffected
        {
            get { return (int)_totalExpectedRowsAffectedField.GetValue(this); }
            set { _totalExpectedRowsAffectedField.SetValue(this, value); }
        }
        private SqlClientSqlCommandSet _currentBatch
        {
            get { return (SqlClientSqlCommandSet)_currentBatchField.GetValue(this); }
            set { _currentBatchField.SetValue(this, value); }
        }
        private StringBuilder _currentBatchCommandsLog
        {
            get { return (StringBuilder) _currentBatchCommandsLogField.GetValue(this); }
            set { _currentBatchCommandsLogField.SetValue(this, value); }
        }
        private int _batchSize
        {
            get { return BatchSize; }
        }
		// ReSharper restore InconsistentNaming

		private SqlClientSqlCommandSet CreateConfiguredBatch()
		{
			return (SqlClientSqlCommandSet)_createConfiguredBatchMethod.Invoke(this, null);
		}
		private void ClearCurrentBatch()
		{
			_clearCurrentBatch.Invoke(this, null);
		}

		public ReliableSqlClientBatchingBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
            : base(connectionManager, interceptor)
        {
            _connectionManager = connectionManager;
        }
		#endregion


        // Need this method call in this class rather than the base class to ensure Prepare is called... if only it was virtual :(
        protected void ExecuteBatch(IDbCommand ps)
        {
            #region NHibernate code
            Log.Debug("Executing batch");
            CheckReaders();
            Prepare(_currentBatch.BatchCommand);
            if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
            {
                Factory.Settings.SqlStatementLogger.LogBatchCommand(_currentBatchCommandsLog.ToString());
                _currentBatchCommandsLog = new StringBuilder().AppendLine("Batch commands:");
            }

            int rowsAffected;
            try
            {
                rowsAffected = _currentBatch.ExecuteNonQuery();
            }
            catch (DbException e)
            {
                throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
            }

            Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected, (DbCommand)ps);

            _currentBatch.Dispose();
            _totalExpectedRowsAffected = 0;
            _currentBatch = CreateConfiguredBatch();
            #endregion
        }

		async Task PrepareReliableAsync(DbCommand cmd, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();
			try
			{
				var sessionConnection = await (_connectionManager.GetConnectionAsync(cancellationToken)).ConfigureAwait(false);
                if (sessionConnection is ReliableSqlDbConnection reliableSqlDbConnection)
				{
					sessionConnection = reliableSqlDbConnection.ReliableConnection.Current;
				}

				if (cmd.Connection != null)
				{
					// make sure the commands connection is the same as the Sessions connection
					// these can be different when the session is disconnected and then reconnected
					if (cmd.Connection != sessionConnection)
					{
						cmd.Connection = sessionConnection;
					}
				}
				else
				{
					cmd.Connection = sessionConnection;
				}

				_connectionManager.EnlistInTransaction(cmd);
				Driver.PrepareCommand(cmd);
			}
			catch (InvalidOperationException ioe)
			{
				throw new ADOException("While preparing " + cmd.CommandText + " an error occurred", ioe);
			}
		}

		protected override Task DoExecuteBatchAsync(DbCommand ps, CancellationToken cancellationToken)
		{
			var connection = (ReliableSqlDbConnection)_connectionManager.GetConnection();
            return ReliableAdoTransaction.ExecuteWithRetryAsync(connection, () => ExcuteBatchAsync(ps, cancellationToken));
		}

		private async Task ExcuteBatchAsync(DbCommand ps, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();
			try
			{
				Log.Debug("Executing batch");
				await (CheckReadersAsync(cancellationToken)).ConfigureAwait(false);
				await (PrepareReliableAsync(_currentBatch.BatchCommand, cancellationToken)).ConfigureAwait(false);
				if (Factory.Settings.SqlStatementLogger.IsDebugEnabled)
				{
					Factory.Settings.SqlStatementLogger.LogBatchCommand(_currentBatchCommandsLog.ToString());
				}
				int rowsAffected;
				try
				{
					rowsAffected = _currentBatch.ExecuteNonQuery();
				}
				catch (DbException e)
				{
					throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
				}

				Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected, ps);
			}
			finally
			{
				ClearCurrentBatch();
			}
		}


		/// <summary>
		/// Prepares the <see cref="DbCommand"/> for execution in the database.
		/// </summary>
		/// <remarks>
		/// This takes care of hooking the <see cref="DbCommand"/> up to an <see cref="DbConnection"/>
		/// and <see cref="DbTransaction"/> if one exists.  It will call <c>Prepare</c> if the Driver
		/// supports preparing commands.
		/// </remarks>
		protected new void Prepare(DbCommand cmd)
        {
            try
            {
                var sessionConnection = (ReliableSqlDbConnection)_connectionManager.GetConnection();

                #region NHibernate code
                if (cmd.Connection != null)
                {
                    // make sure the commands connection is the same as the Sessions connection
                    // these can be different when the session is disconnected and then reconnected
                    if (cmd.Connection != sessionConnection)
                    {
                        cmd.Connection = (System.Data.SqlClient.SqlConnection) sessionConnection;
                    }
                }
                else
                {
                    cmd.Connection = (System.Data.SqlClient.SqlConnection) sessionConnection;
                }

                _connectionManager.EnlistInTransaction(cmd);
                Driver.PrepareCommand(cmd);
                #endregion
            }
            catch (InvalidOperationException ioe)
            {
                #region NHibernate code
                throw new ADOException("While preparing " + cmd.CommandText + " an error occurred", ioe);
                #endregion
            }
        }

        protected override void DoExecuteBatch(DbCommand ps)
        {
            var connection = (ReliableSqlDbConnection)_connectionManager.GetConnection();
            ReliableAdoTransaction.ExecuteWithRetry(connection, () => ExecuteBatch(ps));
        }
    }
}