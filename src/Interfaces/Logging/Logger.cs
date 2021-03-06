﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Nybus.Logging
{
    public class Logger : ILogger
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly string _name;
        private ILogger[] _loggers;

        public Logger(ILoggerFactory loggerFactory, string name)
        {
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));
            if (name == null) throw new ArgumentNullException(nameof(name));

            _loggerFactory = loggerFactory;
            _name = name;

            var providers = loggerFactory.GetProviders();
            _loggers = new ILogger[providers.Count];

            for (var index = 0; index != providers.Count; index++)
            {
                _loggers[index] = providers[index].CreateLogger(name);
            }
        }

        public void Log(LogLevel level, IDictionary<string, object> state, Exception exception)
        {
            if (level >= _loggerFactory.MinimumLevel)
            {
                List<Exception> exceptions = null;

                foreach (var logger in _loggers)
                {
                    try
                    {
                        if (logger.IsEnabled(level))
                        {
                            logger.Log(level, state, exception);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (exceptions == null)
                        {
                            exceptions = new List<Exception>();
                        }

                        exceptions.Add(ex);
                    }
                }

                if (exceptions != null && exceptions.Count > 0)
                {
                    throw new AggregateException("An error occurred while writing to logger(s).", exceptions);
                }
            }
        }

        public bool IsEnabled(LogLevel level)
        {
            if (level < _loggerFactory.MinimumLevel)
            {
                return false;
            }

            List<Exception> exceptions = null;

            foreach (var logger in _loggers)
            {
                try
                {
                    if (logger.IsEnabled(level))
                    {
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    if (exceptions == null)
                    {
                        exceptions = new List<Exception>();
                    }

                    exceptions.Add(ex);
                }
            }

            if (exceptions != null && exceptions.Count > 0)
            {
                throw new AggregateException("An error occurred while writing to logger(s).", exceptions);
            }

            return false;
        }

        public void AddProvider(ILoggerProvider provider)
        {
            var logger = provider.CreateLogger(_name);
            _loggers = _loggers.Concat(new[] {logger}).ToArray();
        }
    }
}